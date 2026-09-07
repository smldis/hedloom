"""Walk a Plan and run it.

This is the piece that was missing while a Plan could be authored and a single
invocation could be executed, but nothing joined the two: the loop lived in an
example. It is deliberately its own unit, because deciding *when* work runs is
a different responsibility from owning one attempt's durable record, and
because the obvious alternative — letting Dask decide readiness — should be a
replacement for this unit rather than a rewrite of another.

What it owns: dependency order, readiness, threading each invocation's outputs
to the inputs that reference them, and what to do when something fails.

What it does not own: attempt identity, journals, transports, reuse (all
`hedloom_exec`), and the Plan itself (`hedloom_flow`). It also does not branch on
results. Every plan it can run was fully determined before it started, which is
what makes a rerun predictable; result-dependent control remains an open
architectural question rather than something smuggled in here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from hedloom_exec.planned import prepare_invocations
from hedloom_exec.transport import Transport

# Binding rules are shared with the Dask kernel rather than restated, so that
# changing which kernel decides readiness cannot change what a plan means.
from hedloom_run.binding import (
    UnsupportedPlacement,
    available_transports,
)

__all__ = ["InvocationOutcome", "RunReport", "UnsupportedPlacement", "run_plan"]


@dataclass(frozen=True, slots=True)
class InvocationOutcome:
    """What happened to one invocation in one run."""

    invocation_id: str
    authored_key: str | None
    operation: str
    input_digest: str | None
    disposition: str
    outcome: str
    placement: str | None = None
    value: Any = None
    artifacts: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    record: str | None = None
    """The record this invocation selected, or None if it never reached one."""
    try_number: int | None = None
    """The try whose evidence was published or reused, if one was selected.

    With ``record`` this is the exact execution, stated by the run rather than
    guessed at afterwards: it is what to pin, prune around, or read back. A preselection refusal or blocked invocation leaves both None. A transport
    refusal after allocation retains the selected try.
    """
    error: str | None = None
    observation_errors: tuple[str, ...] = ()
    block_reason: str | None = None

    @property
    def reused(self) -> bool:
        return self.disposition == "reused"

    @property
    def ran(self) -> bool:
        return self.disposition in ("claimed", "attached")


@dataclass(frozen=True, slots=True)
class RunReport:
    """The whole run, in the order the plan determined."""

    outcomes: tuple[InvocationOutcome, ...]

    @property
    def succeeded(self) -> bool:
        return all(item.outcome == "succeeded" for item in self.outcomes)

    @property
    def ran(self) -> tuple[InvocationOutcome, ...]:
        return tuple(item for item in self.outcomes if item.ran)

    @property
    def reused(self) -> tuple[InvocationOutcome, ...]:
        return tuple(item for item in self.outcomes if item.reused)

    @property
    def blocked(self) -> tuple[InvocationOutcome, ...]:
        return tuple(item for item in self.outcomes if item.outcome == "blocked")

    def summary(self) -> str:
        return "\n".join(
            f"{item.disposition:>9}  {item.authored_key or item.invocation_id:<20}"
            f"  {item.outcome}"
            for item in self.outcomes
        )


def run_plan(
    document: Mapping[str, Any],
    transport: Transport | None = None,
    *,
    transports: Mapping[str, Transport] | None = None,
    root: str,
    workspace_root: str | None = None,
    commands: Mapping[str, Sequence[str]] | None = None,
    outputs: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
    identity_env: Mapping[str, str] | None = None,
    source_fingerprints: Mapping[str, str] | None = None,
    source_addresses: Mapping[str, str] | None = None,
    stop_on_failure: bool = True,
    on_event: Callable[[InvocationOutcome], None] | None = None,
    execution_owner: Any = None,
    on_execution: Any = None,
) -> RunReport:
    """Execute every invocation in a Plan, in dependency order.

    ``commands`` and ``outputs`` bind operations to how they actually run: a
    command line, and which files or streams count as its results. The Plan
    declares meaning; a run binds mechanism. Operations absent from both are
    executed in-process by the transport.

    ``transports`` maps a policy name to the substrate that provides it, so
    each invocation lands where its Plan says it should: one point may take a
    dedicated LSF job while cheap reductions stay local. Passing a single
    ``transport`` instead provides every placement, which is convenient for a
    uniform run and wrong as soon as placements differ. A placement no
    transport provides is fatal, never a silent fallback.

    ``source_addresses`` locates each source the Plan declares, so an operation
    naming an external file as an input is handed it. Resolving an address is
    the caller's authority, never this unit's: omitting the mapping leaves such
    an input resolving to nothing, which is what every run did before.

    Work whose inputs are unchanged since a previous run is reused rather than
    repeated.

    A failure always blocks whatever named its result as an input, whatever
    ``stop_on_failure`` says: those inputs do not exist. ``stop_on_failure``
    decides the rest — with it, nothing further starts at all; without it,
    branches independent of the failure run to the end. That is the same scope
    the graph kernel uses, so a study means the same thing under either.
    """

    from hedloom_run.graph import _RunConfig, _run_ready
    available = available_transports(transport, transports)
    items = prepare_invocations(document, commands=commands, outputs=outputs, identity_env=identity_env,
                                source_fingerprints=source_fingerprints)
    config = _RunConfig(root=root, workspace_root=workspace_root, outputs=outputs,
                        sources=dict(source_addresses or {}))
    return _run_ready(items, available, config, None, execution_owner,
                      on_execution, on_event, stop_on_failure)
