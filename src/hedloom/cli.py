"""Operator commands for run discovery, computation evidence, pins and retention."""

from __future__ import annotations

import argparse
from dataclasses import replace, asdict
import json
import sys
from typing import Sequence

from hedloom_exec.journal import AttemptJournal, JournalError
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
    parser.add_argument("--site", help="site TOML naming both storage directories")
    parser.add_argument("--records-dir", help="attempt record directory")
    parser.add_argument("--work-dir", help="try work directory")


def _storage_dirs(arguments: argparse.Namespace) -> tuple[str, str]:
    if arguments.site:
        if arguments.records_dir or arguments.work_dir:
            raise ValueError("--site cannot be combined with explicit storage directories")
        site = Site.from_file(arguments.site)
        if site.work_dir is None:
            raise ValueError(
                "the site declares no work_dir; this operation needs records_dir and work_dir"
            )
        return site.records_dir, site.work_dir
    if not arguments.records_dir or not arguments.work_dir:
        raise ValueError("this operation needs --site or both --records-dir and --work-dir")
    return arguments.records_dir, arguments.work_dir


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
    for group in ("runs", "attempts"):
        parent = commands.add_parser(group, help="inspect durable history" if group == "runs" else "inspect all computation tries")
        actions = parent.add_subparsers(dest="action", required=True)
        for action in (("list", "show", "path") if group == "runs" else ("list", "show")):
            leaf = actions.add_parser(action)
            if group == "runs":
                source = leaf.add_mutually_exclusive_group(required=True)
                source.add_argument("--site", help="site TOML naming the runs directory")
                source.add_argument("--runs-dir", help="saved runs directory")
            else:
                leaf.add_argument("--site", required=True)
            if action != "path":
                leaf.add_argument("--json", action="store_true")
            if action == "list":
                leaf.add_argument("--since")
                if group == "runs":
                    leaf.add_argument("--name")
                    leaf.add_argument("--study")
                else:
                    leaf.add_argument("--outcome")
            elif group == "runs":
                leaf.add_argument("run_id")
                leaf.add_argument("--invocation", required=action == "path")
                if action == "path":
                    target = leaf.add_mutually_exclusive_group(required=True)
                    target.add_argument("--workspace", action="store_true")
                    target.add_argument("--journal-dir", action="store_true")
            else:
                leaf.add_argument("--record", required=True)
                leaf.add_argument("--try", dest="try_number", type=int, required=True)
    return parser


def _pin(arguments: argparse.Namespace) -> int:
    try:
        records_dir, work_dir = _storage_dirs(arguments)
        record, tries = resolve_selector(records_dir, arguments.selector)
        if not tries:
            raise PinSelectionError(
                f"record {record.identity} has no terminal try to pin"
            )
        journal = AttemptJournal(records_dir, record.identity)
        for item in tries:
            made = pin_workspace(
                journal, try_number=item.number, work_dir=work_dir,
                reason=arguments.reason, actor=arguments.actor,
                freeze=not arguments.no_freeze,
            )
            print(f"{made.pin_id}  {made.identity}#{made.try_number}  {made.reason}")
        return 0
    except (ValueError, PinError, SiteError) as error:
        print(f"hedloom pin: {error}", file=sys.stderr)
        return 2


def _pin_matches(records_dir: str, selector: str):
    found = []
    for record in scan_attempts(records_dir):
        state = AttemptJournal(records_dir, record.identity).fold()
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
        records_dir, _work_dir = _storage_dirs(arguments)
        record, selected = _pin_matches(records_dir, arguments.selector)
        released = unpin_workspace(
            AttemptJournal(records_dir, record.identity), pin_id=selected.pin_id,
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
        records_dir, _work_dir = _storage_dirs(arguments)
        for record in scan_attempts(records_dir):
            state = AttemptJournal(records_dir, record.identity).fold()
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
    records = scan_attempts(site.records_dir if site else arguments.records_dir)
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
        records_dir, work_dir = _storage_dirs(arguments)
        policy, records = _prune_policy(arguments)
        found = survey(records_dir, policy, work_dir=work_dir, records=records)
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


def _discover(arguments):
    from hedloom.discovery import RunHistory, list_attempts
    try:
        site = Site.from_file(arguments.site) if arguments.site else None
        if arguments.command == "attempts":
            options = ({"since": arguments.since, "outcome": arguments.outcome}
                       if arguments.action == "list" else
                       {"record": arguments.record, "try_number": arguments.try_number})
            data = list_attempts(site.records_dir, work_dir=site.work_dir, **options)
            if arguments.action == "show" and not data:
                raise ValueError("selected record/try is unavailable")
        else:
            history = RunHistory(site.runs_dir if site is not None else arguments.runs_dir)
            if arguments.action == "path":
                print(history.resolve_path(arguments.run_id, arguments.invocation,
                      workspace=arguments.workspace, journal_dir=arguments.journal_dir))
                return 0
            if arguments.action == "list":
                data = {"runs": [asdict(row) for row in history.list_runs(
                        name=arguments.name, study=arguments.study, since=arguments.since)],
                        "preparations": history.preparations()}
            elif arguments.invocation:
                data = asdict(history.invocation(arguments.run_id, arguments.invocation))
            else:
                data = asdict(history.read_run(arguments.run_id))
                data["outputs"] = history.outputs(arguments.run_id)
                data["reproducibility"] = history.reproducibility(arguments.run_id)
        if arguments.json:
            print(json.dumps(data, sort_keys=True))
        elif arguments.command == "runs" and arguments.action == "list":
            for row in data["runs"]:
                print(f"{row['run_id']}  {row['study_name']}  {row['run_reported_outcome']}  history:{row['history_status']}")
            for preparation in data["preparations"]:
                print(f"incomplete preparation: {preparation}")
        elif arguments.command == "runs":
            if "run_id" in data:
                print(f"{data['run_id']}  study:{data['study_name']}  "
                      f"reported:{data['run_reported_outcome']}  history:{data['history_status']}")
                print(f"last observation: {data['last_observation']}")
                reproducibility = data.get("reproducibility")
                print(f"reproducibility: {reproducibility['status'] if reproducibility else 'not recorded'}")
                if reproducibility:
                    for gap in reproducibility.get("gaps", []):
                        print(f"reproducibility gap: {gap}")
                rows = data["invocations"]
                for diagnostic in data["diagnostics"]:
                    print(f"history diagnostic: {diagnostic}")
            else:
                rows = [data]
            print("invocation  reported  execution  record#try  workspace")
            for row in rows:
                reference = f"{row['record']}#{row['try_number']}" if row['record'] else "none"
                print(f"{row['address']}  {row['run_reported_outcome']}  "
                      f"{row['selected_execution_state'] or 'unreported'}  {reference}  "
                      f"{row['workspace_status']}: {row['workspace'] or '-'}")
                if row['block_reason']:
                    print(f"  blocked: {row['block_reason']}")
                if row['error']:
                    print(f"  error: {row['error']}")
                for diagnostic in row['diagnostics']:
                    print(f"  diagnostic: {diagnostic}")
        else:
            print("record#try  operation  state  standing  payload")
            for row in data:
                print(f"{row['record']}#{row['try_number']}  {row['operation']}  "
                      f"{row['state']}  {row['standing']}  {row['payload']}")
        return 0
    except (ValueError, KeyError, OSError, JournalError) as error:
        print(f"hedloom {arguments.command}: {error}", file=sys.stderr)
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    """Run the operator CLI, returning a process exit status."""

    parser = _parser()
    supplied = list(sys.argv[1:] if argv is None else argv)
    obsolete = {"--root": "--records-dir", "--workspace-root": "--work-dir",
                "--history-root": "--runs-dir"}
    for argument in supplied:
        flag = argument.split("=", 1)[0]
        if flag in obsolete:
            parser.error(f"{flag} was renamed to {obsolete[flag]}")
    arguments = parser.parse_args(supplied)
    if arguments.command in ("runs", "attempts"):
        return _discover(arguments)
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
