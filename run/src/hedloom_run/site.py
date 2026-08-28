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

`hedloom_exec` identifies a source by its declared address and contract, never by what
is at that address — deliberately, since it resolves no addresses and should
not start. The consequence was that editing an input file in place changed
nothing: every downstream invocation was reused, and a study reported results
computed from a file that no longer existed in that form. A run knows what an
address space means, so a run is where the fingerprint belongs.

Sources are **hashed**, not stat'ed. The register's reason for preferring
`mtime` plus size was the cost of hashing multi-GB raw *outputs*; an authored
input is a small text or JSON document, and hashing kilobytes costs nothing
while being immune to the mtime churn an ordinary `git checkout` causes.
Anything implausibly large for an authored input falls back to size and mtime,
and says which it used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import blake2b
from pathlib import Path
from typing import Any, Callable, Mapping
from copy import deepcopy
import os

from hedloom_exec.planned import source_references
from hedloom_exec.transport import SubmissionRefused, Transport

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
    placements: Mapping[str, int | Mapping[str, Any]] = field(default_factory=dict)
    """Each placement this site offers: its budget, and how to reach it.

    Two forms, and the second is the same vocabulary a profile's `[placement.x]`
    table uses — deliberately, so that a site written in Python and a site read
    from TOML are the same declaration in two notations rather than two code
    paths that must agree::

        placements={"local": 4}                        # a bare budget
        placements={"lsf": {"kind": "lsf-interactive", # a whole placement
                            "queue": "reg", "walltime": "1",
                            "cores": 1, "max_jobs": 2}}
        placements={"pool": {"kind": "lsf-pooled",     # reusable farm workers
                             "queue": "short", "cores": 1, "memory_mb": 4000,
                             "walltime": "2:00",
                             "workers": 20, "max_jobs": 20}}

    The two LSF kinds differ in what one farm job *is*. `lsf-interactive`
    submits one `bsub -I` per invocation, so each one is an individually
    visible, cancellable, accountable job. `lsf-pooled` holds `workers` jobs
    open and routes many invocations through them, paying queue dispatch once
    per worker instead of once per invocation — and giving up everything that
    needs an invocation to be a job. See `hedloom_run.pooled` for when that trade is worth
    taking; it is a per-operation judgement, not a site-wide one, which is why
    both kinds normally appear in the same profile.

    A mapping that names a `kind` also builds that placement's transport, so the
    budget and the substrate are declared once, together, and cannot be given
    inconsistent names. Before this, Python callers had to pass `transports=`
    and `placements=` separately, naming the placement twice with nothing
    checking that the two agreed — a typo produced a budget with no transport,
    found much later as `UnsupportedPlacement`.

    `transports=` remains for substrates no mapping can describe: an in-process
    placement needs callables, which is what `with_transports` supplies. An
    explicitly supplied transport wins over one built from a declaration.

    After construction every entry reads as a declaration, with `max_jobs`
    filled in and validated. A site keeps what it was told and `capacity`
    derives the numbers from it, because keeping the declaration is what lets a
    single run override *how* a placement is reached without restating it.

    `max_jobs` is the number that makes a placement real at run time. Each is one
    Dask worker whose threads are that placement's budget and nothing else's,
    and every task the graph kernel submits asks for one unit of the placement
    it resolved to. A local invocation therefore cannot occupy a thread meant
    for a farm job, which it otherwise can and does: an unrestricted task is
    legal on every worker, and Dask both places and *steals* on that basis.

    For an LSF placement this is the share of the farm hedloom may spend, which
    is **not** the site's MAX JOB policy. That policy caps everything you have
    running under your user, hedloom's jobs and the ones you submit by any other
    means together, so declaring all of it here means one of the two waits for
    the other. When it is hedloom that waits, the cost is not only delay: its
    worker threads are held by `bsub -I` clients that are still queued, so the
    placement spends its own budget on waiting rather than on work.

    Leave headroom for whatever else you run. It is not a tuning knob this
    project owns — the number is a judgement about how you use the farm.
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

    retention: Mapping[str, Any] = field(default_factory=dict)
    """Operator-owned workspace retention policy from ``[retention]``."""

    def __post_init__(self) -> None:
        """Anchor every root, because a relative one is silently wrong.

        A command is run *in* its attempt's workspace and told where to write
        by a path built from that same workspace. Absolute, those agree.
        Relative, the command resolves the path a second time against the
        directory it was just placed in, and writes nowhere — reported as a
        tool that could not open its own output file, which is a long way
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
        # A bare budget is the one-key declaration `{"max_jobs": cap}`, so both
        # notations reach the same validator and produce the same refusals.
        declared = {
            name: (dict(options) if isinstance(options, Mapping) else {"max_jobs": options})
            for name, options in self.placements.items()
        }
        describes = {
            name: options for name, options in declared.items() if "kind" in options
        }
        if describes:
            object.__setattr__(
                self,
                "transports",
                # Explicit wins: an in-process placement's callables cannot be
                # described by a mapping and must not be displaced by one.
                {**_transports_from(describes), **dict(self.transports)},
            )
        # Every plan can name `local`, because that is what an operation
        # declaring no policy resolves to at authoring time. A site that never
        # mentions it still has to be able to run it, so the capacity exists
        # whether or not the profile said so — otherwise the commonest plan of
        # all asks for a placement the cluster does not offer.
        caps = _placements_from(declared, self.threads)
        declared.setdefault("local", {})
        object.__setattr__(
            self,
            "placements",
            {
                name: {**options, "max_jobs": caps.get(name, self.threads or 1)}
                for name, options in declared.items()
            },
        )
        # Refused here rather than when a cluster is built: a profile naming an
        # exposure this installation cannot honour is a configuration error,
        # and finding it at construction means finding it before a study runs.
        if self.dashboard not in EXPOSURES:
            raise SiteError(
                f"this site declares dashboard = {self.dashboard!r}, which is "
                f"not one of {', '.join(repr(item) for item in EXPOSURES)}"
            )
        if not isinstance(self.retention, Mapping):
            raise SiteError("retention policy must be a mapping")
        retained = deepcopy(dict(self.retention))
        if retained:
            from hedloom_exec.prune import RetentionError, RetentionPolicy

            try:
                RetentionPolicy.from_toml(retained)
            except RetentionError as error:
                raise SiteError(f"invalid retention policy: {error}") from error
        object.__setattr__(self, "retention", retained)

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
            retention=self.retention,
        )

    @property
    def capacity(self) -> dict[str, int]:
        """How many invocations each placement may have in flight at once."""

        return {
            name: int(options["max_jobs"])
            for name, options in self.placements.items()
        }

    def overridden(self, patch: Mapping[str, Mapping[str, Any]]) -> "Site":
        """A site that runs work differently and means exactly the same by it.

        The patch speaks the profile's own vocabulary — `placement` and
        `kernel` — and merges into each placement's declaration, so one run can
        spend less of the farm, ask for another queue, or open no dashboard
        without a second profile and without editing the first.

        Why this is safe rather than a back door into the Plan: none of it is
        identity-bearing. `reuse.py` settles that deliberately — "queue,
        walltime, cores, and host do not change what a deterministic operation
        produces, so changing them must not invalidate a result" — so an
        overridden run lands on the same attempt identities as a plain one, and
        the two reuse each other's work. Anything in `IDENTITY_KEYS` belongs to
        the author and cannot be reached from here.

        `study` is refused rather than accepted: relocating the record changes
        what a run reuses, which is a different installation and not a
        different way of running this one.
        """

        unknown = sorted(set(patch) - {"placement", "kernel"})
        if unknown:
            raise SiteError(
                f"a run may override {', '.join(unknown)} nowhere: an override "
                "says how this run executes, never what it means. It may carry "
                "'placement' and 'kernel'. Roots belong to the site, because "
                "moving them changes what is reused; inputs, commands and "
                "outputs belong to the Plan."
            )
        placements = {
            name: dict(options) for name, options in self.placements.items()
        }
        for name, options in (patch.get("placement") or {}).items():
            if name not in placements:
                raise SiteError(
                    f"the override names placement {name!r}, which this site "
                    f"does not offer. It offers: {', '.join(sorted(placements))}"
                )
            placements[name] = {**placements[name], **dict(options)}
        kernel = patch.get("kernel") or {}
        # A transport built from a declaration is rebuilt from the patched one;
        # anything supplied by hand is kept, since no mapping describes it.
        described = {
            name
            for name, options in self.placements.items()
            if options.get("kind") not in (None, "in-process")
        }
        return Site(
            root=self.root,
            transports={
                name: transport
                for name, transport in self.transports.items()
                if name not in described
            },
            workspace_root=self.workspace_root,
            address_spaces=self.address_spaces,
            placements=placements,
            threads=kernel.get("threads", self.threads),
            dashboard=kernel.get("dashboard", self.dashboard),
            retention=self.retention,
        )

    def served_in_process(self) -> "Site":
        """The same placements, every one of them run by the authored body here.

        For debugging a farm study on the submit host: the placement names, the
        budgets and therefore the Plan are untouched, but nothing leaves the
        process. Identity is untouched too, which is the point and also the
        catch — a local run publishes attempts a later farm run will reuse. That
        is sound exactly as far as the author's declared inputs go; a result
        that depends on the machine needs that fact in `identity_env`, which is
        what it is for.
        """

        return Site(
            root=self.root,
            transports={},
            workspace_root=self.workspace_root,
            address_spaces=self.address_spaces,
            placements={
                name: {**options, "kind": "in-process"}
                for name, options in self.placements.items()
            },
            threads=self.threads,
            dashboard=self.dashboard,
            retention=self.retention,
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
            for name, cap in self.capacity.items()
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
            # Handed over whole. The constructor builds the transports and the
            # budgets from this one table, so a profile and a `Site(...)` written
            # by hand cannot diverge in what a placement means.
            placements=data.get("placement") or {},
            threads=(data.get("kernel") or {}).get("threads"),
            dashboard=(data.get("kernel") or {}).get("dashboard", "network"),
            retention=data.get("retention") or {},
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

    from hedloom_exec.lsf import LSFInteractiveTransport, PLACEMENT_OPTIONS
    from hedloom_run.pooled import LSFPooledTransport, POOL_OPTIONS

    built: dict[str, Transport] = {}
    kernel_keys = {"kind", "max_jobs"}
    mechanic_keys = {"timeout"}
    for name, options in placements.items():
        settings = dict(options)
        kind = settings.get("kind")
        # The vocabulary is per kind, because the kinds genuinely differ. A
        # pool cannot honour `licences` or `resources`: its workers are claimed
        # before any invocation is routed to them, so a per-invocation request
        # would be accepted and then silently ignored, which is worse than
        # being refused.
        vocabulary = POOL_OPTIONS if kind == "lsf-pooled" else PLACEMENT_OPTIONS
        unknown = sorted(
            set(settings) - kernel_keys - mechanic_keys - set(vocabulary)
        )
        if unknown:
            raise SiteError(
                f"placement {name!r} declares unknown option "
                f"{', '.join(unknown)}; transport options are "
                f"{', '.join(vocabulary)}"
            )
        if kind == "lsf-pooled":
            # Data only. The pooled transport must ship to a readiness worker,
            # so it holds no client; the live one is built on the worker by
            # `hedloom_run.pooled.PooledClientPlugin`. See that module for why
            # that is a rule rather than a convenience.
            built[name] = LSFPooledTransport(
                name,
                settings={
                    key: value for key, value in settings.items()
                    if key in POOL_OPTIONS
                },
            )
        elif kind == "lsf-interactive":
            defaults = {
                key: value for key, value in settings.items()
                if key in PLACEMENT_OPTIONS
            }
            try:
                built[name] = LSFInteractiveTransport(
                    defaults=defaults,
                    timeout=settings.get("timeout"),
                )
            except (SubmissionRefused, TypeError, ValueError) as error:
                raise SiteError(
                    f"placement {name!r} cannot build its transport: {error}"
                ) from error
        elif kind == "in-process":
            # Needs callables; supplied through with_transports(...).
            continue
        else:
            raise SiteError(
                f"placement {name!r} names an unknown kind {kind!r}; this site "
                "can build 'lsf-interactive' and 'lsf-pooled' from "
                "configuration, and 'in-process' placements must be given "
                "their implementations"
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
            if kind in ("lsf-interactive", "lsf-pooled"):
                raise SiteError(
                    f"placement {name!r} declares no max_jobs. Each placement "
                    "becomes one worker whose threads are that placement's "
                    "budget, so the number decides how many farm jobs may be in "
                    "flight at once. Set it to the share of the farm hedloom "
                    "may spend, leaving room for whatever else you submit — it "
                    "is not the site's MAX JOB policy, which caps your jobs "
                    "from every source together. There is no safe default to "
                    "guess."
                    + (
                        " For a pool it is how many invocations may be in "
                        "flight against it, which is a different number from "
                        "'workers' — how many LSF jobs the pool holds open. "
                        "Usually you want them equal; when they differ, the "
                        "smaller one binds and the other is a lie."
                        if kind == "lsf-pooled"
                        else ""
                    )
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
