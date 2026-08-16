"""Everything about a run that is not the study.

A Plan says what to compute. It deliberately does not say which queue exists
here, where records are kept, or what `"repository-relative"` means on this
machine — those are facts about an installation, and putting them in a Plan
would make a study unportable the moment it was authored.

They have to live somewhere, though, and until now they lived as loose
arguments at every call site. A `Site` is that somewhere: placements to
substrates, the roots records and workspaces are written under, and the address
spaces a declared source is resolved through.

Resolving addresses is what unlocks the correctness fix this module exists for.
The invariant:

    A source's identity must change when its content changes.

`hedloom_exec` identifies a source by its declared address and codec, never by what
is at that address — deliberately, since it resolves no addresses and should
not start. The consequence was that editing an input netlist in place changed
nothing: every downstream invocation was reused, and a study reported results
computed from a file that no longer existed in that form. A run knows what an
address space means, so a run is where the fingerprint belongs.

Sources are **hashed**, not stat'ed. The register's reason for preferring
`mtime` plus size was the cost of hashing multi-GB raw *outputs*; an authored
input is a netlist or a JSON document, and hashing kilobytes costs nothing
while being immune to the mtime churn an ordinary `git checkout` causes.
Anything implausibly large for an authored input falls back to size and mtime,
and says which it used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import blake2b
from pathlib import Path
from typing import Any, Callable, Mapping
import os

from hedloom_exec.planned import source_references
from hedloom_exec.transport import Transport

__all__ = [
    "EXPOSURES",
    "PLACEMENT_RESOURCE",
    "Site",
    "SiteError",
    "fingerprint_file",
    "fingerprint_sources",
]

_MAX_HASHED_BYTES = 64 * 1024 * 1024
_CHUNK = 1 << 20

PLACEMENT_RESOURCE = "placement:"
"""Prefix for the Dask resource that carries a placement to the graph kernel.

One name, written here, read by `hedloom_run.cluster` when it declares a worker's
capacity and by `hedloom_run.graph` when it annotates a task. Both derive it from
the same profile reading, so the cluster and the plan cannot disagree about what
a placement is called — a disagreement would not be a wrong number, it would be
a task no worker is allowed to run, which Dask never schedules and never
reports.
"""

EXPOSURES = ("network", "loopback", "none")
"""Every exposure a profile may declare, most open first.

Vocabulary rather than behaviour, which is why it lives with the profile it is
read from; `hedloom_run.cluster` turns each value into a cluster.
"""


class SiteError(RuntimeError):
    """The installation cannot answer something a run needs to know."""


def fingerprint_file(path: Path) -> str:
    """Identify a file by its content, or say plainly that it did not.

    The prefix is part of the value. Two runs that fingerprinted the same file
    by different methods must not look identical, or a study could silently
    reuse across a change the cheaper method could not see.
    """

    stat = path.stat()
    if stat.st_size > _MAX_HASHED_BYTES:
        # Implausible for an authored input. Recorded honestly rather than
        # hashed: this is the weaker signal and the name says so.
        return f"stat:{stat.st_size}:{stat.st_mtime_ns}"
    digest = blake2b(digest_size=16)
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(_CHUNK), b""):
            digest.update(chunk)
    return f"blake2b:{digest.hexdigest()}"


@dataclass(frozen=True, slots=True)
class Site:
    """One installation: where work runs, where records go, what addresses mean."""

    root: str
    transports: Mapping[str, Transport] = field(default_factory=dict)
    workspace_root: str | None = None
    address_spaces: Mapping[str, str] = field(default_factory=dict)
    placements: Mapping[str, int] = field(default_factory=dict)
    """How many invocations each placement may have in flight at once.

    The number that makes a placement real at run time. Each entry becomes one
    Dask worker whose threads are that placement's budget and nothing else's,
    and every task the graph kernel submits asks for one unit of the placement
    it resolved to. A local invocation therefore cannot occupy a thread meant
    for a farm job, which it otherwise can and does: an unrestricted task is
    legal on every worker, and Dask both places and *steals* on that basis.

    For an LSF placement this is the site's MAX JOB policy for this user. It is
    not a tuning knob this project owns — ask for the number.
    """

    threads: int | None = None
    """Local concurrency, and nothing to do with the farm.

    How many in-process invocations may run at once on the submit host. It is
    the default cap for an in-process placement that declares no `max_jobs` of
    its own; farm concurrency is that placement's `max_jobs`, which is a
    different fact about a different machine.
    """

    dashboard: str = "network"
    """How much of the graph kernel's cluster this installation exposes.

    A Dask scheduler and every worker open an HTTP listener on all interfaces,
    whether or not anyone opens a browser, and a shared submit host has other
    users on it. `"network"` is Dask's own behaviour and the default, so
    nothing changes for an installation that declares nothing; `"loopback"`
    keeps the dashboard off the network; `"none"` opens no socket at all.

    Read by `hedloom_run.cluster`, which is also where the values are defined and
    where a multi-process cluster is refused a silence it cannot have.
    """

    def __post_init__(self) -> None:
        """Anchor every root, because a relative one is silently wrong.

        A command is run *in* its attempt's workspace and told where to write
        by a path built from that same workspace. Absolute, those agree.
        Relative, the command resolves the path a second time against the
        directory it was just placed in, and writes nowhere — reported as a
        simulator that could not open its own output file, which is a long way
        from the truth.

        `from_file` already anchors relative paths to the profile directory, so
        this only closes the gap for a `Site` built in Python.
        """

        object.__setattr__(self, "root", str(Path(self.root).resolve()))
        if self.workspace_root is not None:
            object.__setattr__(
                self, "workspace_root", str(Path(self.workspace_root).resolve())
            )
        object.__setattr__(
            self,
            "address_spaces",
            {
                name: str(Path(location).resolve())
                for name, location in self.address_spaces.items()
            },
        )
        # Every plan can name `local`, because that is what an operation
        # declaring no policy resolves to at authoring time. A site that never
        # mentions it still has to be able to run it, so the capacity exists
        # whether or not the profile said so — otherwise the commonest plan of
        # all asks for a placement the cluster does not offer.
        placements = {name: int(cap) for name, cap in self.placements.items()}
        placements.setdefault("local", self.threads or 1)
        object.__setattr__(self, "placements", placements)
        # Refused here rather than when a cluster is built: a profile naming an
        # exposure this installation cannot honour is a configuration error,
        # and finding it at construction means finding it before a study runs.
        if self.dashboard not in EXPOSURES:
            raise SiteError(
                f"this site declares dashboard = {self.dashboard!r}, which is "
                f"not one of {', '.join(repr(item) for item in EXPOSURES)}"
            )

    def with_transports(self, **transports: Transport) -> "Site":
        """Add substrates a configuration file cannot describe.

        An in-process placement needs Python callables, which no TOML holds.
        This seam closes when operations carry their own implementations.
        """

        return Site(
            root=self.root,
            transports={**self.transports, **transports},
            workspace_root=self.workspace_root,
            address_spaces=self.address_spaces,
            placements=self.placements,
            threads=self.threads,
            dashboard=self.dashboard,
        )

    def cluster_spec(self) -> dict[str, dict[str, Any]]:
        """One worker per placement, sized by that placement's own cap.

        The thread count is *derived* rather than configured, because the two
        are independent gates on the same worker and the smaller one binds
        silently: a worker declaring capacity for two hundred farm jobs and
        holding eight threads runs eight, reports nothing, and looks correct.

        Read by `hedloom_run.cluster.cluster_for`, which is the only supported way
        to build a cluster this kernel will accept — the annotation on a task
        and the capacity on a worker come from this one reading, so they cannot
        drift apart.
        """

        return {
            name: {
                "nthreads": cap,
                "resources": {f"{PLACEMENT_RESOURCE}{name}": cap},
            }
            for name, cap in self.placements.items()
        }

    def resolve(self, address: Mapping[str, Any]) -> Path:
        """Turn a declared address into a path on this machine."""

        space = address.get("address_space")
        locator = address.get("locator")
        if space not in self.address_spaces:
            raise SiteError(
                f"this site does not define the address space {space!r} "
                f"(defines: {', '.join(sorted(self.address_spaces)) or 'none'}). "
                "A source cannot be located, so a run cannot tell whether its "
                "inputs changed."
            )
        return Path(self.address_spaces[space]) / str(locator)

    def fingerprints(self, document: Mapping[str, Any]) -> dict[str, str]:
        """Identify every source this Plan declares, by content."""

        return fingerprint_sources(document, self.resolve)

    def source_addresses(
        self, document: Mapping[str, Any], fingerprints: Mapping[str, str]
    ) -> dict[str, str]:
        """Locate every declared source, keyed as its input bindings name it.

        This is what lets a body declare an external file as an input and be
        handed it. Resolution happens here, on the machine that submits, using
        the same address space fingerprinting already read — so a run resolves
        an address in one place, and a path that reaches a farm job is a path
        this site could see.

        ``fingerprints`` is required rather than optional because the key each
        source takes is derived from it. Passing a different mapping than the
        run uses would name strings nothing looks up, and sources would resolve
        to nothing again without saying so.
        """

        return {
            reference: str(self.resolve(source.get("address") or {}))
            for reference, source in source_references(document, fingerprints).items()
        }

    @classmethod
    def from_file(cls, path: str | os.PathLike[str]) -> "Site":
        """Read a site profile from TOML.

        Relative paths resolve against the profile's own directory, not the
        working directory: a study run from elsewhere must mean the same thing.
        """

        try:
            import tomllib
        except ModuleNotFoundError as error:  # pragma: no cover - Python 3.10
            raise SiteError(
                "reading a site profile needs tomllib (Python 3.11+); "
                "construct Site(...) directly on older runtimes"
            ) from error

        profile = Path(path).resolve()
        with open(profile, "rb") as handle:
            data = tomllib.load(handle)

        base = profile.parent

        def anchored(value: str) -> str:
            return str(base / value) if not os.path.isabs(value) else value

        study = data.get("study") or {}
        if "root" not in study:
            raise SiteError(f"{profile} declares no [study] root")

        return cls(
            root=anchored(study["root"]),
            workspace_root=(
                anchored(study["workspace_root"])
                if study.get("workspace_root")
                else None
            ),
            address_spaces={
                name: anchored(location)
                for name, location in (data.get("address_space") or {}).items()
            },
            transports=_transports_from(data.get("placement") or {}),
            placements=_placements_from(
                data.get("placement") or {}, (data.get("kernel") or {}).get("threads")
            ),
            threads=(data.get("kernel") or {}).get("threads"),
            dashboard=(data.get("kernel") or {}).get("dashboard", "network"),
        )


def _transports_from(
    placements: Mapping[str, Mapping[str, Any]],
) -> dict[str, Transport]:
    """Build the substrates a profile can describe, and refuse the rest.

    Only kinds whose configuration is entirely data belong here. A placement
    naming an unknown kind is refused rather than skipped: a run that quietly
    lacked a placement would fail later as `UnsupportedPlacement`, blaming the
    Plan for what is a configuration error.
    """

    from hedloom_exec.lsf import LSFInteractiveTransport

    built: dict[str, Transport] = {}
    for name, options in placements.items():
        settings = dict(options)
        kind = settings.pop("kind", None)
        # Read by `_placements_from` into the cluster's shape, never by a
        # transport: how many jobs may be in flight is a fact about this site's
        # policy, not an argument to any one `bsub`.
        settings.pop("max_jobs", None)
        if kind == "lsf-interactive":
            built[name] = LSFInteractiveTransport(**settings)
        elif kind == "in-process":
            # Needs callables; supplied through with_transports(...).
            continue
        else:
            raise SiteError(
                f"placement {name!r} names an unknown kind {kind!r}; this site "
                "can build 'lsf-interactive' from configuration, and "
                "'in-process' placements must be given their implementations"
            )
    return built


def _placements_from(
    placements: Mapping[str, Mapping[str, Any]], threads: int | None
) -> dict[str, int]:
    """How many invocations each declared placement may have in flight.

    Refuses an uncapped farm placement rather than choosing for you. The two
    plausible defaults are both wrong in a way that is expensive to discover:
    an arbitrary small number silently throttles a sweep, and an arbitrary
    large one authorises more concurrent jobs than the site permits and more
    live `bsub` clients than the submit host will carry.
    """

    caps: dict[str, int] = {}
    for name, options in placements.items():
        kind = options.get("kind")
        declared = options.get("max_jobs")
        if declared is None:
            if kind == "lsf-interactive":
                raise SiteError(
                    f"placement {name!r} declares no max_jobs. Each placement "
                    "becomes one worker whose threads are that placement's "
                    "budget, so the number decides how many farm jobs may be in "
                    "flight at once. Set it from this site's LSF MAX JOB policy "
                    "for your user; there is no safe default to guess."
                )
            declared = threads or 1
        if not isinstance(declared, int) or isinstance(declared, bool) or declared < 1:
            raise SiteError(
                f"placement {name!r} declares max_jobs = {declared!r}; it must be "
                "a positive whole number of concurrent invocations"
            )
        caps[name] = declared
    return caps


def fingerprint_sources(
    document: Mapping[str, Any],
    resolve: Callable[[Mapping[str, Any]], Path],
) -> dict[str, str]:
    """Fingerprint each declared source, keyed by the Plan's source id.

    A declared source that cannot be found is fatal, and fatal *early*: the
    alternative is a run that reuses results computed from a file nobody can
    show you.
    """

    fingerprints: dict[str, str] = {}
    for source in document.get("sources", []):
        address = source.get("address") or {}
        path = resolve(address)
        if not path.exists():
            raise SiteError(
                f"source {source.get('id')!r} declares {address.get('locator')!r} "
                f"in address space {address.get('address_space')!r}, which "
                f"resolves to {path} and does not exist"
            )
        if path.is_dir():
            # A directory-tree source: identify it by the content of everything
            # under it, so editing one file inside invalidates the study.
            digest = blake2b(digest_size=16)
            for item in sorted(path.rglob("*")):
                if item.is_file():
                    digest.update(str(item.relative_to(path)).encode())
                    digest.update(fingerprint_file(item).encode())
            fingerprints[source["id"]] = f"tree:{digest.hexdigest()}"
        else:
            fingerprints[source["id"]] = fingerprint_file(path)
    return fingerprints
