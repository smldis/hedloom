"""Exceptions shared across the attempt protocol's internal layers."""

__all__ = ["AttemptError"]


class AttemptError(RuntimeError):
    """The attempt cannot proceed under its recorded state."""
