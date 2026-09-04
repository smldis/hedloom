"""The one mutable thing this example has, kept where a body can reach it.

Not a style choice. An operation body is copied to the worker that runs it, and
a body defined in ``__main__`` is copied *by value* — every global it names
travels with it, which a live `Session` cannot survive (it holds locks). A
module is pickled by reference instead, so naming the session as
``state.SESSION`` ships the reference and leaves the session where it is.
"""

from __future__ import annotations

from typing import Any

SESSION: Any = None
