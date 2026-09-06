"""Exceptions shared across the attempt protocol's internal layers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hedloom_exec.attempt import Selection

__all__ = ["AttemptError", "ExecutionFailure"]


class ExecutionFailure(RuntimeError):
    """A protocol failure, with the selection reached before it failed.

    Exec fills these fields at its selection boundary, independently of whether
    a publisher is installed or succeeds. Preselection failures leave them empty.
    """

    selection: Selection | None = None
    publication_errors: tuple[str, ...] = ()


class AttemptError(ExecutionFailure):
    """The attempt cannot proceed under its recorded state."""
