"""Operator commands over attempt records: pinning and reclamation.

Everything here addresses a record or one of its tries. There is no
name-shaped selector and no current-output view, because a record holds a
computation rather than belonging to a study: two studies declaring the same
work reach the same record, so a `<study>:<key>` address could only have named
whichever of them arrived first. A caller that has just run something already
holds the exact reference — `InvocationOutcome.record` and `.try_number`.

Finding a record without one is discovery, and discovery is not built yet.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
import sys
from typing import Sequence

from hedloom_exec.journal import AttemptJournal
from hedloom_exec.pins import (
    PinError,
    PinSelectionError,
    pin as pin_workspace,
    resolve_selector,
    unpin as unpin_workspace,
)
from hedloom_exec.prune import (
    RetentionError, RetentionPolicy, RetentionRule, _size, survey,
)
from hedloom_exec.reuse import scan_attempts
from hedloom_run.site import Site, SiteError


def _storage_location(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--site", help="site TOML naming both storage roots")
    parser.add_argument("--root", help="attempt-record root")
    parser.add_argument("--workspace-root", help="try-workspace root")


def _storage_roots(arguments: argparse.Namespace) -> tuple[str, str]:
    if arguments.site:
        if arguments.root or arguments.workspace_root:
            raise ValueError("--site cannot be combined with explicit roots")
        site = Site.from_file(arguments.site)
        if site.workspace_root is None:
            raise ValueError(
                "the site declares no workspace_root; this operation needs both roots"
            )
        return site.root, site.workspace_root
    if not arguments.root or not arguments.workspace_root:
        raise ValueError("this operation needs --site or both --root and --workspace-root")
    return arguments.root, arguments.workspace_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hedloom")
    commands = parser.add_subparsers(dest="command", required=True)

    pin = commands.add_parser("pin", help="protect terminal try workspaces")
    _storage_location(pin)
    pin.add_argument(
        "selector", help="record identity or unique prefix, optionally #<try>"
    )
    pin.add_argument("--reason", required=True)
    pin.add_argument("--actor")
    pin.add_argument("--no-freeze", action="store_true")

    unpin = commands.add_parser("unpin", help="release one pin")
    _storage_location(unpin)
    unpin.add_argument("selector", help="pin id or unique pin-id prefix")
    unpin.add_argument("--reason", required=True)
    unpin.add_argument("--actor")
    unpin.add_argument("--no-thaw", action="store_true")

    pins = commands.add_parser("pins", help="list active pins")
    _storage_location(pins)

    prune = commands.add_parser("prune", help="survey or reclaim try workspaces")
    _storage_location(prune)
    prune.add_argument("--rule")
    prune.add_argument("--outcome")
    prune.add_argument("--failed", action="store_true")
    prune.add_argument("--older-than")
    prune.add_argument("--larger-than")
    prune.add_argument("--keep-latest", type=int)
    prune.add_argument(
        "--record", help="restrict to one record identity or unique prefix"
    )
    prune.add_argument("--apply", action="store_true")
    prune.add_argument("--json", action="store_true")
    prune.add_argument("--limit-bytes")
    return parser


def _pin(arguments: argparse.Namespace) -> int:
    try:
        root, workspace_root = _storage_roots(arguments)
        record, tries = resolve_selector(root, arguments.selector)
        if not tries:
            raise PinSelectionError(
                f"record {record.identity} has no terminal try to pin"
            )
        journal = AttemptJournal(root, record.identity)
        for item in tries:
            made = pin_workspace(
                journal, try_number=item.number, workspace_root=workspace_root,
                reason=arguments.reason, actor=arguments.actor,
                freeze=not arguments.no_freeze,
            )
            print(f"{made.pin_id}  {made.identity}#{made.try_number}  {made.reason}")
        return 0
    except (ValueError, PinError, SiteError) as error:
        print(f"hedloom pin: {error}", file=sys.stderr)
        return 2


def _pin_matches(root: str, selector: str):
    found = []
    for record in scan_attempts(root):
        state = AttemptJournal(root, record.identity).fold()
        found.extend(
            (record, item) for item in state.pins
            if item.is_active and item.pin_id.startswith(selector)
        )
    if not found:
        raise PinSelectionError(f"no active pin matches {selector!r}")
    if len(found) != 1:
        raise PinSelectionError(
            f"pin selector {selector!r} is ambiguous: "
            + ", ".join(item.pin_id for _record, item in found)
        )
    return found[0]


def _unpin(arguments: argparse.Namespace) -> int:
    try:
        root, _workspace_root = _storage_roots(arguments)
        record, selected = _pin_matches(root, arguments.selector)
        released = unpin_workspace(
            AttemptJournal(root, record.identity), pin_id=selected.pin_id,
            reason=arguments.reason, actor=arguments.actor,
            thaw=not arguments.no_thaw,
        )
        print(f"released {released.pin_id}  {released.identity}#{released.try_number}")
        return 0
    except (ValueError, PinError, SiteError) as error:
        print(f"hedloom unpin: {error}", file=sys.stderr)
        return 2


def _pins(arguments: argparse.Namespace) -> int:
    try:
        root, _workspace_root = _storage_roots(arguments)
        for record in scan_attempts(root):
            state = AttemptJournal(root, record.identity).fold()
            for item in state.pins:
                if item.is_active:
                    print(
                        f"{item.pin_id}  {item.identity}#{item.try_number}  "
                        f"{item.actor}  {item.reason}"
                    )
        return 0
    except (ValueError, PinError, SiteError) as error:
        print(f"hedloom pins: {error}", file=sys.stderr)
        return 2


def _prune_policy(arguments: argparse.Namespace) -> tuple[RetentionPolicy, tuple]:
    site = Site.from_file(arguments.site) if arguments.site else None
    declared = RetentionPolicy.from_toml(site.retention if site else {})
    rules = list(declared.rules)
    if arguments.rule:
        rules = [item for item in rules if item.name == arguments.rule]
        if not rules:
            raise RetentionError(f"no retention rule is named {arguments.rule!r}")

    outcomes = None
    if arguments.failed:
        outcomes = ("failed", "cancelled")
    if arguments.outcome:
        if outcomes is not None:
            raise RetentionError("--failed and --outcome cannot be combined")
        outcomes = tuple(item.strip() for item in arguments.outcome.split(",") if item.strip())
    selection_override = any(
        value is not None
        for value in (outcomes, arguments.older_than, arguments.larger_than)
    )
    overrides = {
        "outcome": outcomes,
        "older_than": arguments.older_than,
        "larger_than": arguments.larger_than,
        "keep_latest": arguments.keep_latest,
    }
    if selection_override and not arguments.rule:
        rules = [RetentionRule(
            "command-line", outcome=outcomes or (),
            older_than=arguments.older_than, larger_than=arguments.larger_than,
            keep_latest=arguments.keep_latest if arguments.keep_latest is not None else 1,
        )]
    elif rules:
        rules = [
            replace(rule, **{key: value for key, value in overrides.items()
                             if value is not None})
            for rule in rules
        ]
    if not rules:
        raise RetentionError(
            "no retention rule was selected; declare one in the site or use "
            "--outcome, --failed, --older-than, or --larger-than"
        )
    records = scan_attempts(site.root if site else arguments.root)
    if arguments.record is not None:
        records = tuple(
            item for item in records
            if item.identity.startswith(arguments.record)
        )
        if not records:
            raise RetentionError(f"no record matches {arguments.record!r}")
    return RetentionPolicy(tuple(rules), floor=declared.floor), records


def _prune(arguments: argparse.Namespace) -> int:
    try:
        root, workspace_root = _storage_roots(arguments)
        policy, records = _prune_policy(arguments)
        found = survey(root, policy, workspace_root=workspace_root, records=records)
        if not arguments.apply:
            data = found.as_data()
            if arguments.json:
                print(json.dumps(data, sort_keys=True))
            else:
                print(found.summary())
                for item in found.candidates:
                    print(
                        f"candidate {item.identity}#{item.try_number}  "
                        f"{item.bytes} bytes  {item.rule}"
                    )
            return 0
        limit = (
            _size(arguments.limit_bytes, field="--limit-bytes")
            if arguments.limit_bytes else None
        )
        report = found.apply(limit_bytes=limit)
        data = {
            "applied_at": report.applied_at,
            "freed_bytes": report.freed_bytes,
            "stopped_at_limit": report.stopped_at_limit,
            "removed": [
                {"identity": item.identity, "try": item.try_number,
                 "workspace": str(item.workspace), "bytes": item.bytes,
                 "rule": item.rule}
                for item in report.removed
            ],
            "skipped": [
                {"identity": item.identity, "try": item.try_number,
                 "reason": item.reason, "detail": item.detail}
                for item in report.skipped
            ],
        }
        if arguments.json:
            print(json.dumps(data, sort_keys=True))
        else:
            print(
                f"removed {len(report.removed)} workspace(s), "
                f"freed {report.freed_bytes} byte(s)"
            )
        return 0
    except (ValueError, RetentionError, SiteError) as error:
        print(f"hedloom prune: {error}", file=sys.stderr)
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    """Run the operator CLI, returning a process exit status."""

    arguments = _parser().parse_args(argv)
    if arguments.command == "pin":
        return _pin(arguments)
    if arguments.command == "unpin":
        return _unpin(arguments)
    if arguments.command == "pins":
        return _pins(arguments)
    if arguments.command == "prune":
        return _prune(arguments)
    raise AssertionError(f"unhandled command {arguments.command!r}")


if __name__ == "__main__":  # pragma: no cover
    # Without this, `python -m hedloom.cli` imports the module, runs nothing,
    # and exits zero — a check that cannot fail is worse than one that does.
    raise SystemExit(main())
