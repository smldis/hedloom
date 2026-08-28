"""One file: the operator's loop, driven entirely through the `hedloom` command.

    python examples/cli.py

The other examples are what an author writes. This is what an operator types
afterwards, when the study is running and the questions are about a *path*:
where is the current output, is the file I opened yesterday still the current
one, and what replaced it.

    hedloom where   resolve the current output for an authored key
    hedloom check   is this path still current, or has it been superseded
    hedloom log     every iteration at one key, and what changed each time
    hedloom prune   what storage is spent, as a dry run
    hedloom pin     protect one try workspace, and list what is protected

The tour is a real one: a study runs, one point's inputs are edited, and the
same commands are asked again. `check` answering `behind` after the edit — with
a non-zero exit status, so a script can act on it — is the whole reason the
command exists, and it is the question `latest/` alone cannot answer.

Every command below is really executed and its real output printed. The exit
statuses are checked, because a query command that answered wrongly and exited
zero would look identical to one that worked.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import (  # noqa: E402
    Site,
    file,
    flow,
    local,
    operation,
    parameter,
    shell,
    study,
    sweep,
)
from hedloom_exec.reuse import scan_attempts  # noqa: E402

POINTS = ({"key": "narrow", "width": 4}, {"key": "wide", "width": 40})

# The edit an operator actually makes: one point's inputs change, the other is
# left alone. The untouched point must stay current, so `check` has to
# distinguish "superseded" from "not the newest thing in the root".
REVISED = ({"key": "narrow", "width": 6}, {"key": "wide", "width": 40})


@operation(
    config={"key": parameter(str), "width": parameter(int)},
    outputs={"table": file("table.txt", kind="squares-table")},
)
def tabulate(out, *, key: str, width: int):
    """A launcher: the tool writes the declared file, as a real tool would."""

    return shell(
        "sh",
        "-c",
        f"awk 'BEGIN {{ for (i = 0; i < {width}; i++) print i, i * i }}' "
        f"> '{out.table}'",
    )


@flow
def tabulation(points):
    """One keyed scope per point, so every output has an authored name."""

    return {
        point["key"]: tabulate(key=point["key"], width=point["width"]).table
        for point in sweep(points, key="key")
    }


@study(name="cli-tour", default_policy=local())
def squares(points):
    """The study: two tables, in this process."""

    return tabulation.named("tables")(points)


SHORTEN: dict[str, str] = {}


def hedloom(*arguments: str, expect: int = 0) -> subprocess.CompletedProcess:
    """Run one `hedloom` command for real, print it as typed, and check it.

    Invoked as `python -m hedloom.cli` so the example runs from a checkout
    without installing anything; an installed copy answers to `hedloom`
    directly, which is the form printed here. Storage roots are printed as the
    shell variables announced above, because a page-wide absolute path hides
    the command it is part of.
    """

    def short(item: str) -> str:
        for value, name in SHORTEN.items():
            if item == value:
                return name
            if item.startswith(value + "/"):
                return name + item[len(value):]
        return item

    shown = " ".join(short(item) for item in arguments)
    print(f"\n    $ hedloom {shown}")
    finished = subprocess.run(
        [sys.executable, "-m", "hedloom.cli", *arguments],
        capture_output=True,
        text=True,
        check=False,
        env={"PYTHONPATH": ":".join(sys.path)},
    )
    for line in (finished.stdout + finished.stderr).splitlines():
        print(f"      {line}")
    if finished.returncode != expect:
        print(f"      FAILED: expected exit {expect}, got {finished.returncode}")
    return finished


def main(work: Path | None = None) -> int:
    if shutil.which("awk") is None or shutil.which("sh") is None:
        print("awk and sh are not both on PATH; this example needs real tools")
        return 1

    default = Path(__file__).resolve().parent / "_runs" / "cli"
    work = Path(work) if work is not None else default
    shutil.rmtree(work, ignore_errors=True)
    records = work / "attempts"
    workspaces = work / "work"
    site = Site(root=str(records), workspace_root=str(workspaces))
    where = ("--root", str(records))
    both = (*where, "--workspace-root", str(workspaces))
    SHORTEN[str(records)] = "$ROOT"
    SHORTEN[str(workspaces)] = "$WORK"
    print(f"    ROOT={records}")
    print(f"    WORK={workspaces}")
    print("    (a site profile carries both, so `--site site.toml` replaces them)\n")

    print("=== a study runs, and now there are paths to ask about")
    first = squares(POINTS).submit(site=site)
    if not first.succeeded:
        print(f"    FAILED: {first.summary()}")
        return 1
    # The plan's id is derived from the study, so the example reads it back
    # rather than restating it: a selector that only worked because it was
    # copied from the code would not be evidence of anything.
    plan_id = next(item.plan_id for item in scan_attempts(records))
    narrow = f"{plan_id}:narrow:tabulate"
    print(f"    two points tabulated; the authored key is {narrow!r}")

    print("\n=== where: the current output, as a path to hand a tool")
    located = hedloom("where", narrow, "--output", "table", *where)
    held = Path(located.stdout.strip())
    if not held.is_file():
        print(f"    FAILED: `where` named {held}, which is not a readable file")
        return 1
    print(f"    that path reads back {len(held.read_text().splitlines())} row(s)")

    print("\n=== check: is the path I am holding still the current one?")
    hedloom("check", str(held), *where)

    print("\n=== the operator edits one point's inputs and reruns")
    second = squares(REVISED).submit(site=site)
    if not second.succeeded:
        print(f"    FAILED: {second.summary()}")
        return 1
    print("    the edited point is a new record; the untouched point is reused")

    print("\n=== check again: the held path is behind, and says what changed")
    # Exit 1 rather than 0 is the point. `check` is meant to be scriptable, so
    # "behind" has to be a status a shell can branch on, not just a word.
    stale = hedloom("check", str(held), *where, expect=1)
    if "behind" not in stale.stdout:
        print("    FAILED: the held path was superseded and `check` did not say so")
        return 1

    wide = f"{plan_id}:wide:tabulate"
    print("\n=== and the point nobody touched is still current")
    hedloom("check", str(hedloom("where", wide, "--output", "table", *where)
                          .stdout.strip()), *where)

    print("\n=== log: every iteration at one key, newest marked")
    history = hedloom("log", narrow, *where)
    if history.stdout.count("\n") < 2:
        print("    FAILED: two iterations were run; `log` showed fewer")
        return 1

    print("\n=== prune: what storage is spent, without spending anything")
    surveyed = hedloom("prune", *both, "--older-than", "0s", "--keep-latest", "0")
    # Nothing, and that is the contract rather than a disappointment. Every
    # table here succeeded, and a record whose standing evidence is still valid
    # is never a candidate -- superseded or not, a later run with those inputs
    # would reuse it. Pruning frees work that failed or was cancelled;
    # `examples/retention.py` is the one that actually reclaims bytes.
    if "0 candidate" not in surveyed.stdout:
        print("    FAILED: everything here succeeded, so nothing is reclaimable")
        return 1
    print("    nothing: a result a later run could reuse is not spent storage")

    print("\n=== pin: an authored key with two iterations is ambiguous, and says so")
    # The refusal is the feature. Two records answer to this key now, and
    # picking the newer one silently is exactly how an operator ends up
    # protecting the output they were not looking at.
    hedloom("pin", narrow, "--reason", "quoted in a report", *both, expect=2)

    print("\n=== pin: so name the iteration — the superseded one, being quoted")
    # `log` marks the current iteration with `*` and indents the rest, so the
    # identity is the first field on a superseded line and the second on the
    # current one.
    superseded = next(
        parts[0]
        for parts in (line.split() for line in history.stdout.splitlines())
        if parts and parts[0] != "*"
    )
    pinned = hedloom(
        "pin", superseded, "--reason", "quoted in a report", *both
    )
    hedloom("pins", *both)
    pin_id = pinned.stdout.split()[0] if pinned.stdout.split() else ""
    if not pin_id:
        print("    FAILED: pinning by identity produced no pin to release")
        return 1
    hedloom("unpin", pin_id, "--reason", "the report shipped", *both)

    print("\n    every command above ran for real; `--json` is available on "
          "`prune` for scripts that would rather parse than read")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
