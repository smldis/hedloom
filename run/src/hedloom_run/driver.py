"""Immutable consumer outcomes and Plan-ordered reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


from hedloom_run.binding import UnsupportedPlacement

__all__ = ["InvocationOutcome", "RunReport", "UnsupportedPlacement"]


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
