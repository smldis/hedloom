"""A study: one object that can show you its Plan and then run it.

The two halves of this system were always joined by hand. An author wrote a
Plan; someone else wrote a binding supplying implementations, commands, output
paths, transports and roots; a third caller walked the result. Each seam was a
place where the study could mean something different from what was authored.

`submit()` joins them. It is deliberately not a new capability: it materializes
the same Plan, binds the same operations, and calls the same kernel. What it
removes is the opportunity to bind them inconsistently.

The invariant:

    Nothing is spent until `submit`, and what runs is what the Plan showed.

`study.plan` is complete and inspectable before submission — the manifesto's
requirement to materialize jobs before spending compute — and the
operations that run are the ones the Plan names, because both come from the
same declaration rather than from two files that must agree.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping as MappingABC
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Thread
from typing import Any, Callable, Mapping
import warnings

from hedloom_exec.prune import RetentionPolicy, survey
from hedloom_exec.transport import Transport, TransportError
from hedloom_exec.watch import AttemptStatus, LSFStatusReader, live_attempts, observe
from hedloom_run.binding import output_value
from hedloom_run.driver import InvocationOutcome, RunReport, run_plan
from hedloom_run.site import Site

from hedloom.binding import BoundTransport

__all__ = [
    "OutputUnavailable",
    "Study",
    "StudyOutput",
    "StudyRun",
    "start_watcher",
    "submit",
]

_WATCH_INTERVAL_SECONDS = 10.0
_WATCH_JOIN_SECONDS = 1.0
_WATCH_THREAD_NAME = "hedloom-watch"


class OutputUnavailable(RuntimeError):
    """An exported output exists in the Plan but this run did not produce it.

    Raised rather than returned, because every value a body may legitimately
    return is also a plausible answer here. ``None`` from a succeeded
    invocation is a result; ``None`` from one that failed is the absence of a
    result, and a caller that cannot tell them apart draws conclusions from
    work that never happened.
    """


@dataclass(frozen=True, slots=True)
class StudyOutput:
    """One output the Plan exported, with the outcome that produced it.

    The reference and the outcome are kept rather than resolved away, so a
    caller can ask which invocation an output came from, whether it was reused,
    and why it is missing, without guessing an authored key.
    """

    name: str
    reference: Mapping[str, Any]
    outcome: InvocationOutcome | None

    @property
    def invocation_id(self) -> str | None:
        """The invocation the Plan says produces this output."""

        return self.reference.get("invocation_id")

    @property
    def output_name(self) -> str | None:
        """Which of that invocation's declared outputs this export names."""

        return self.reference.get("output_name")

    @property
    def authored_key(self) -> str | None:
        """The producer, addressed the way the study was authored."""

        return self.outcome.authored_key if self.outcome is not None else None

    @property
    def available(self) -> bool:
        """Whether the producing invocation succeeded in this run."""

        return self.outcome is not None and self.outcome.outcome == "succeeded"

    @property
    def value(self) -> Any:
        """What this exported output port resolved to.

        A file or directory output resolves to its recorded address rather than
        its bytes: the artifact is the result, and reading it is the caller's
        decision. This is the same resolution a downstream input receives,
        because it is the same rule, shared with both kernels in
        ``hedloom_run.binding``.
        """

        outcome = self._produced()
        return output_value(outcome.artifacts, outcome.value, self.output_name)

    @property
    def artifact(self) -> Mapping[str, Any] | None:
        """The recorded artifact for this port, or ``None`` if it has none.

        A file or directory output records its address, size and modification
        time. An output whose operation declared nothing about where it lands
        records nothing, and its result is the returned value.
        """

        return self._produced().artifacts.get(self.output_name)

    def _produced(self) -> InvocationOutcome:
        """The outcome that produced this output, or refuse to invent one."""

        if self.reference.get("type") != "output":
            raise OutputUnavailable(
                f"output {self.name!r} is not produced by an invocation: the "
                f"Plan exports a reference of type "
                f"{self.reference.get('type')!r}, which this façade does not "
                "resolve to a result"
            )
        if self.outcome is None:
            raise OutputUnavailable(
                f"output {self.name!r} names invocation "
                f"{self.invocation_id!r}, which this run did not report"
            )
        if self.outcome.outcome != "succeeded":
            where = self.outcome.authored_key or self.outcome.invocation_id
            detail = f": {self.outcome.error}" if self.outcome.error else ""
            raise OutputUnavailable(
                f"output {self.name!r} was not produced: {where} "
                f"{self.outcome.outcome}{detail}"
            )
        return self.outcome


class StudyOutputs(MappingABC):
    """Exported outputs under the names the author gave them, and nothing else."""

    __slots__ = ("_entries",)

    def __init__(self, entries: Mapping[str, StudyOutput]) -> None:
        self._entries = dict(entries)

    def __getitem__(self, name: str) -> StudyOutput:
        try:
            return self._entries[name]
        except KeyError:
            exported = ", ".join(self._entries) or "none"
            raise KeyError(
                f"this study exports no output named {name!r}; "
                f"exports: {exported}"
            ) from None

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return f"StudyOutputs({sorted(self._entries)})"


@dataclass(frozen=True, slots=True)
class StudyRun:
    """What a run produced, addressable under ``study_name`` as authored.

    Outcomes and exported outputs are evidence. Whether an evaluation passed is
    the value it returned; whether the engineering conclusion is accepted is
    neither of those, and nothing here decides it.
    """

    report: RunReport
    document: Mapping[str, Any]
    study_name: str

    def __getitem__(self, authored_key: str) -> InvocationOutcome:
        for outcome in self.report.outcomes:
            if outcome.authored_key == authored_key:
                return outcome
        raise KeyError(f"no invocation was authored with key {authored_key!r}")

    @property
    def outputs(self) -> Mapping[str, StudyOutput]:
        """What this study exported, under the names the author gave them.

        The author decides what a study produces, so neither report order nor
        completion order appears here: appending an unrelated invocation
        changes nothing about what the outputs mean. A Plan exporting nothing
        has an empty mapping, and one exporting several keeps them several —
        there is no unwrapping to a single value and no preferred entry.
        """

        by_invocation = {
            outcome.invocation_id: outcome for outcome in self.report.outcomes
        }
        return StudyOutputs(
            {
                exported["name"]: StudyOutput(
                    name=exported["name"],
                    reference=dict(exported.get("reference") or {}),
                    outcome=by_invocation.get(
                        (exported.get("reference") or {}).get("invocation_id")
                    ),
                )
                for exported in self.document.get("outputs", [])
            }
        )

    @property
    def succeeded(self) -> bool:
        """Whether every invocation succeeded. It judges no returned value."""

        return self.report.succeeded

    def summary(self) -> str:
        return self.report.summary()


class Study:
    """A named authored Plan together with the operations that implement it."""

    def __init__(
        self,
        plan: Any,
        implementations: Mapping[str, Callable[..., Any]],
        *,
        name: str,
    ):
        self.plan = plan
        self.implementations = dict(implementations)
        self.name = name

    @property
    def document(self) -> Mapping[str, Any]:
        return self.plan.to_data()

    def summary(self) -> str:
        """What will run, before anything is spent."""

        document = self.document
        invocations = sorted(
            document.get("invocations", []),
            key=lambda item: item.get("authored_key") or item["id"],
        )
        lines = [
            f"study {self.name}\n"
            f"plan schema {document.get('schema_version')}: "
            f"{len(invocations)} invocations, "
            f"{len(document.get('sources', []))} sources"
        ]
        # Widths follow the study rather than a guess: an authored key that
        # overran a fixed column used to run into the operation name, which is
        # unreadable exactly when a study is large enough to need reading.
        keys = [item.get("authored_key") or item["id"] for item in invocations]
        key_width = max((len(item) for item in keys), default=0) + 2
        name_width = (
            max(
                (len(item["operation"]["name"]) for item in invocations),
                default=0,
            )
            + 2
        )
        for invocation, key in zip(invocations, keys):
            placement = (invocation.get("policy") or {}).get("name", "local")
            lines.append(
                f"  {key:<{key_width}}"
                f"{invocation['operation']['name']:<{name_width}}{placement}"
            )
        return "\n".join(lines)

    def submit(
        self,
        *,
        site: Site,
        client: Any = None,
        override: Mapping[str, Mapping[str, Any]] | None = None,
        sequential: bool = False,
        locally: bool = False,
        watch: bool = False,
        stop_on_failure: bool = True,
        on_event: Callable[[InvocationOutcome], None] | None = None,
        _watch_reader: LSFStatusReader | None = None,
    ) -> StudyRun:
        """Run this study, honouring every placement the Plan resolved.

        Concurrency is the site's: this opens the compute the site declares for
        as long as the run needs it, spends up to each placement's `max_jobs`,
        and gives it back. A site that declares nothing has capacity one, which
        is one invocation at a time — there was never a second mode to choose,
        only a second implementation of the same capacity.

        ``override``, ``sequential`` and ``locally`` are the session's, and mean
        exactly what they mean there; this is the one-run form of

            with session(site) as farm:
                farm.submit(study)

        which is what to use for several runs, so they share one cluster, one
        budget and one watcher.

        ``client`` is the escape hatch for a caller who already holds one, and
        skips all of that. Nothing here needs it.

        ``stop_on_failure`` also stops work that has not started, as well as the
        dependents a failure blocks in any case.
        """

        if client is None:
            from hedloom.session import session

            with session(
                site,
                override,
                sequential=sequential,
                locally=locally,
                watch=watch,
                _watch_reader=_watch_reader,
            ) as live:
                return live.submit(
                    self, stop_on_failure=stop_on_failure, on_event=on_event
                )

        return self._run(
            site=site,
            client=client,
            watch=watch,
            stop_on_failure=stop_on_failure,
            on_event=on_event,
            _watch_reader=_watch_reader,
        )

    def _run(
        self,
        *,
        site: Site,
        client: Any = None,
        watch: bool = False,
        stop_on_failure: bool = True,
        on_event: Callable[[InvocationOutcome], None] | None = None,
        _watch_reader: LSFStatusReader | None = None,
    ) -> StudyRun:
        """Bind this study to a site and walk it, on the given client or here.

        The seam a `Session` drives. It owns no lifetime of its own: whatever
        cluster, client or watcher this needs has already been opened by
        whoever called it, which is why the same body serves one run and many.
        """

        document = self.document
        transports = {
            name: BoundTransport(self.implementations, delegate)
            for name, delegate in site.transports.items()
        }
        # A placement declared as in-process has no transport a TOML can build:
        # its implementation is the authored body, which lives here. Supplying
        # one for every placement the site knows about is also what makes
        # `local` work on a profile that only ever mentions a farm queue —
        # every operation that declares no policy resolves to `local`, so a run
        # that cannot provide it refuses the commonest plan there is.
        for name in site.placements:
            transports.setdefault(name, BoundTransport(self.implementations))

        report_to = _reporter(on_event, watch)
        # One reading of every declared source serves both: its content decides
        # whether work is stale, and its location is what the body receives.
        # They are computed together because the second is keyed by the first.
        fingerprints = site.fingerprints(document)
        common = dict(
            transports=transports,
            root=site.root,
            workspace_root=site.workspace_root,
            source_fingerprints=fingerprints,
            source_addresses=site.source_addresses(document, fingerprints),
            stop_on_failure=stop_on_failure,
            on_event=report_to,
        )

        watcher = start_watcher(site.root, _watch_reader) if watch else None
        try:
            if client is None:
                report = run_plan(document, **common)
            else:
                from hedloom_run.graph import run_plan_graph

                report = run_plan_graph(document, client=client, **common)
        finally:
            if watcher is not None:
                stop, thread = watcher
                stop.set()
                # Status is evidence about the run, never part of it. A stuck
                # scheduler query therefore cannot keep the caller here or
                # change an otherwise completed outcome.
                thread.join(timeout=_WATCH_JOIN_SECONDS)
        _apply_automatic_retention(site)
        return StudyRun(
            report=report,
            document=document,
            study_name=self.name,
        )


def _apply_automatic_retention(site: Site) -> None:
    """Apply only the bounded rules the site names; never change a run result."""

    automatic = site.retention.get("automatic") or {}
    names = tuple(automatic.get("after_run") or ())
    if not names:
        return
    try:
        if site.workspace_root is None:
            raise ValueError(
                "automatic retention needs [study] workspace_root as well as root"
            )
        declared = RetentionPolicy.from_toml(site.retention)
        selected = tuple(rule for rule in declared.rules if rule.name in names)
        # Site construction validates names. Keep this check here because a
        # Site built around a mutable mapping must still refuse lost authority.
        missing = sorted(set(names) - {rule.name for rule in selected})
        if missing:
            raise ValueError(
                "automatic retention names unknown rule(s): " + ", ".join(missing)
            )
        policy = RetentionPolicy(selected, floor=declared.floor)
        survey(
            site.root, policy, workspace_root=site.workspace_root
        ).apply(actor="automatic-after-run")
    except Exception as error:  # noqa: BLE001 - retention cannot own run outcome
        warnings.warn(
            f"automatic retention failed after the run: {type(error).__name__}: {error}",
            RuntimeWarning,
            stacklevel=2,
        )


def _reporter(
    on_event: Callable[[InvocationOutcome], None] | None, watch: bool
) -> Callable[[InvocationOutcome], None] | None:
    if on_event is not None:
        return on_event
    if not watch:
        return None

    def report(outcome: InvocationOutcome) -> None:
        name = outcome.authored_key or outcome.invocation_id
        disposition = "reused" if outcome.reused else outcome.disposition
        detail = f"  {outcome.error}" if outcome.error else ""
        print(f"[{disposition:>9}] {name:<32}{outcome.outcome}{detail}")

    return report


def start_watcher(
    root: str | Path, reader: LSFStatusReader | None = None
) -> tuple[Event, Thread]:
    """Start the small queue poller used by ``submit(watch=True)``.

    The injected reader keeps the scheduler boundary explicit in tests. The
    thread is a daemon as a second line of defence behind the bounded join: a
    wedged external command must never acquire authority over process lifetime.
    """

    stop = Event()
    thread = Thread(
        target=_watch,
        args=(root, reader, stop),
        daemon=True,
        name=_WATCH_THREAD_NAME,
    )
    thread.start()
    return stop, thread


def _watch(
    root: str | Path,
    reader: LSFStatusReader | None,
    stop: Event,
) -> None:
    while True:
        try:
            before = live_attempts(root)
            previous = {row.identity: row.observed for row in before}
            rows = observe(root, reader, attempts=before)
        except TransportError as error:
            print(f"[watch disabled] {error}")
            return

        for row in rows:
            state = row.observed
            old_state = previous.get(row.identity)
            if state is not None and state != old_state:
                print(_watch_transition(row, old_state))

        if stop.wait(_WATCH_INTERVAL_SECONDS):
            return


def _watch_transition(row: AttemptStatus, previous: str | None) -> str:
    name = row.job_name or row.identity
    transition = f"{previous} → {row.observed}" if previous else f"→ {row.observed}"
    queued = (
        f" ({row.queue_seconds:.0f}s queued)"
        if row.observed == "running" and row.queue_seconds is not None
        else ""
    )
    return f"[watch] {name} {transition}{queued}"


def submit(
    study: Study,
    *,
    site: Site,
    client: Any = None,
    override: Mapping[str, Mapping[str, Any]] | None = None,
    sequential: bool = False,
    locally: bool = False,
    watch: bool = False,
    stop_on_failure: bool = True,
) -> StudyRun:
    """Run a study. The verb Hedloom Flow reserved and refused until now."""

    return study.submit(
        site=site,
        client=client,
        override=override,
        sequential=sequential,
        locally=locally,
        watch=watch,
        stop_on_failure=stop_on_failure,
    )
