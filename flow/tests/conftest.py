"""Make `from examples import ...` resolve to *this* unit's examples.

`hedloom/examples/` and `flow/examples/` are both namespace packages — neither
has an `__init__.py` — so whichever appears first on `sys.path` wins. Each unit
declares `testpaths = ["tests"]` and is meant to be run from its own directory,
where that is `flow/examples/`. Run from the repository root instead, as
`AGENTS.md`'s combined check does, `hedloom/examples/` shadows it and
`test_acceptance.py` fails at collection with
`cannot import name 'refinement' from 'examples' (unknown location)`.

That looked for a long time like two pre-existing failures in `hedloom-flow`.
It was neither pre-existing nor in `hedloom-flow`: it was the invocation. Two
red errors that mean nothing are worse than none, because they teach a reader
to skip past red.

Prepending the unit root here fixes it from any working directory, and is
local to the unit that needs it.
"""

from __future__ import annotations

import sys
from pathlib import Path

_UNIT_ROOT = str(Path(__file__).resolve().parents[1])

if sys.path[:1] != [_UNIT_ROOT]:
    while _UNIT_ROOT in sys.path:
        sys.path.remove(_UNIT_ROOT)
    sys.path.insert(0, _UNIT_ROOT)
