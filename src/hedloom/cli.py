"""Small operator queries over attempt records and the current-result view."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Sequence

from hedloom_exec.alias import alias_path
from hedloom_exec.lineage import is_behind, lineage, why_reran
from hedloom_exec.reuse import scan_attempts
from hedloom_run.site import Site


def _location(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--site", help="site TOML whose [study] root to query")
    source.add_argument("--root", help="attempt root to query directly")


def _root(arguments: argparse.Namespace) -> str:
    return Site.from_file(arguments.site).root if arguments.site else arguments.root


def _selector(value: str) -> tuple[str, str]:
    plan_id, separator, authored_key = value.partition(":")
    if not separator or not plan_id or not authored_key:
        raise ValueError("selector must be <plan>:<authored-key>")
    return plan_id, authored_key


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hedloom")
    commands = parser.add_subparsers(dest="command", required=True)

    where = commands.add_parser("where", help="resolve a current output")
    _location(where)
    where.add_argument("selector")
    where.add_argument("--output", required=True)

    check = commands.add_parser("check", help="check whether a path is behind")
    _location(check)
    check.add_argument("path")

    log = commands.add_parser("log", help="show an invocation's iterations")
    _location(log)
    log.add_argument("selector")
    return parser


def _where(arguments: argparse.Namespace) -> int:
    try:
        plan_id, authored_key = _selector(arguments.selector)
    except ValueError as error:
        print(f"hedloom where: {error}", file=sys.stderr)
        return 2
    root = _root(arguments)
    if not lineage(root, plan_id=plan_id, authored_key=authored_key):
        print(
            f"hedloom where: no attempt matches {arguments.selector!r}",
            file=sys.stderr,
        )
        return 2
    published = alias_path(
        root,
        plan_id=plan_id,
        authored_key=authored_key,
        output=arguments.output,
    )
    if not published.is_symlink():
        print(
            f"hedloom where: {arguments.selector!r} has no output "
            f"{arguments.output!r}",
            file=sys.stderr,
        )
        return 2
    print(published.resolve(strict=False))
    return 0


def _record_containing(root: str, path: Path):
    resolved = path.resolve(strict=False)
    return next(
        (record for record in scan_attempts(root) if record.identity in resolved.parts),
        None,
    )


def _check(arguments: argparse.Namespace) -> int:
    root = _root(arguments)
    path = Path(arguments.path)
    record = _record_containing(root, path)
    if record is None:
        print(f"hedloom check: {path} is not a recorded attempt path", file=sys.stderr)
        return 2
    if record.plan_id is None or record.authored_key is None:
        print(
            f"hedloom check: {path} has no authored lineage attribution",
            file=sys.stderr,
        )
        return 2
    iterations = lineage(
        root, plan_id=record.plan_id, authored_key=record.authored_key
    )
    matched = next(
        (iteration for iteration in iterations if iteration.identity == record.identity),
        None,
    )
    current = next((iteration for iteration in iterations if iteration.is_current), None)
    if matched is None or current is None:
        print(
            f"hedloom check: {path} has no current output alias",
            file=sys.stderr,
        )
        return 2
    if matched.is_current:
        print(f"current: {record.identity}")
        return 0
    replacement = is_behind(root, path)
    if replacement is None:
        print(f"hedloom check: cannot resolve current iteration", file=sys.stderr)
        return 2
    current_record = next(
        item for item in scan_attempts(root) if item.identity == replacement.identity
    )
    reason = ", ".join(
        why_reran(record.input_digests, current_record.input_digests)
    ) or "identity"
    print(
        f"behind: {record.identity} was superseded by {replacement.identity} "
        f"({reason} changed)"
    )
    return 1


def _log(arguments: argparse.Namespace) -> int:
    try:
        plan_id, authored_key = _selector(arguments.selector)
    except ValueError as error:
        print(f"hedloom log: {error}", file=sys.stderr)
        return 2
    iterations = lineage(
        _root(arguments), plan_id=plan_id, authored_key=authored_key
    )
    if not iterations:
        print(
            f"hedloom log: no attempt matches {arguments.selector!r}",
            file=sys.stderr,
        )
        return 2
    for iteration in iterations:
        marker = "*" if iteration.is_current else " "
        reason = ", ".join(iteration.changed_keys) or "first"
        print(
            f"{marker} {iteration.identity}  {iteration.at}  "
            f"{iteration.outcome or 'unfinished'}  {reason}"
        )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the operator CLI, returning a process exit status."""

    arguments = _parser().parse_args(argv)
    if arguments.command == "where":
        return _where(arguments)
    if arguments.command == "check":
        return _check(arguments)
    if arguments.command == "log":
        return _log(arguments)
    raise AssertionError(f"unhandled command {arguments.command!r}")
