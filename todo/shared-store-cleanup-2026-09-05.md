# Shared computation store: remove the old ownership interfaces

> Historical implementation brief, completed by PR #19 (merge `604f53f`).
> Preserved as the original scope and execution instructions, not a current task.
> Current behavior is documented in the maintained guides and ontolomes; remaining
> discovery and pinning work is summarized in the
> [updated handoff](../DISCOVERABILITY-ATTEMPT-IDENTITY-HANDOFF.md).

User direction after reviewing the first identity implementation, 2026-09-05.
This supersedes the interface-preservation constraints in
[the first plan](attempt-identity-implementation-2026-09-05.md).

The user explicitly wants the old ownership model removed, accepts breaking
changes, and will develop study discoverability separately. A maintainable
shared store is the deliverable; warning messages around misleading legacy
interfaces are not a satisfactory endpoint.

## Required cleanup

1. **Make recorded execution independent of requesting names.** Derive identity
   from the bundle without requiring a study, Plan ID, invocation ID, or authored
   key. Remove the old mandatory parameters/gates and caller plumbing rather
   than retaining compatibility arguments that are ignored. Inspect explicit
   identity overrides too: the normal path should derive the canonical identity;
   any genuinely necessary low-level override must not bypass declaration
   matching. Do not retain an override solely for old tests.

2. **Remove creator-based ownership and implicit study history from Exec.**
   Remove creator-keyed `attempts_for`/`stale_attempts`, automatic `supersedes`
   chains, creator-derived `changed_keys`, and lineage/currentness inference
   from the shared store. Remove obsolete record fields, exports, reporting
   plumbing, and callers. Two computation records coexist; one study choosing
   Y does not make X globally superseded. A pure comparison of two explicitly
   supplied declarations is conceptually valid, but do not preserve an unused
   utility as a compatibility vestige. Do not introduce a generic provenance
   framework merely to keep creator fields alive.

3. **Delete `latest/` machinery and its ownership-based commands.** Remove alias
   creation/resolution/scanning and alias-based retention exemptions. Remove
   `where`, `check`, and creator-history `log` as presently defined, rather than
   maintaining them through fallbacks, warnings, or replacement discovery APIs.
   Remove their public exports and unused modules. Update or remove associated
   examples, tests, documentation, and maintained guidance. This authorizes
   deleting obsolete source/test/example files; it does not authorize deleting
   any existing runtime evidence or artifact roots.

4. **Keep operational storage interfaces record/try based.** Pin selection must
   address records/tries, with unambiguous record prefixes permitted if useful.
   Remove `<study>:<key>` selector support and its transitional explanations.
   Remove creator-based `prune --study` / `--invocation` filtering. Reclamation
   should use record/try properties and pin protection. Preserve claim-locked
   rechecks, live/unreconciled protection, manifest retention, and the existing
   standing-evidence protection; removing output aliases does not remove
   `standing.json`. Likewise, a per-record `keep_latest` retention count is not
   the deleted `latest/` alias feature. Study pin orchestration and discovery
   remain separate work; do not build them here.

5. **Expose the exact selected execution.** Carry a stable record identity and
   selected try reference from recorded execution through invocation outcomes,
   so callers can inspect/pin what was actually executed or reused. Do not infer
   it afterwards by scanning creator names, resolving aliases, or consulting
   mutable standing evidence. Requests that never select a try must not invent
   one. This is the execution result contract, not a study-history schema.

6. **Make maintained contracts coherent.** Update ontolomes, decision ledger,
   public docstrings, CLI help, and guides for the resulting interface. Remove
   the former promise that authored keys are necessary for reuse. Distinguish
   identity rendering changes from unreadable layouts: the first pass still
   reads layout 1, so old hash-derived records are not automatically selected
   by the new hash, which does not itself make their contents unreadable. No
   migration, compatibility aliases, deprecation period, or legacy fallback is
   required. Leave dated historical proposals as historical context.

Authored invocation IDs and keys remain meaningful for Plan topology, binding,
and per-invocation reporting; study names remain meaningful for authoring and
run context. Remove their role as storage ownership, not every occurrence of
the words. Re-read and preserve other agents' current shared-file edits.

## Verification and delivery

- Retain meaningful identity regressions: cross-study/key reuse, producer rename
  stability, declared changes and repetitions, exact record/try references on
  reuse and retry, placement independence, and preserved claim exclusion.
- Add bundle-only recorded execution coverage. Verify old ownership interfaces
  and CLI options are absent, fresh runs create no `latest/`, and direct pin/
  unpin/prune operations work with record/try references. Preserve tests of
  deletion protections while removing tests of the deliberately deleted alias
  and creator-ownership behavior.
- Audit retained imports, fields, APIs, documentation, and examples for hidden
  dependencies on the removed ownership model. This list is a starting point,
  not a reason to stop after a mechanical rename.
- Run focused verification, then from Hedloom:
  `PYTHONPATH=src:flow/src:exec/src:run/src .venv/bin/python -m pytest -q tests exec/tests run/tests flow/tests`.
  Run `git diff --check`. From the composition root use
  `.venv/bin/python composition.py docs` (the root venv has Sphinx; the Hedloom
  venv was found not to). Capture actual exit statuses and inspect warnings.
- Scheduler waiting/coalescing, cancellation policy, nested-study redesign,
  and study-history/discoverability implementation remain deferred. Preserve
  the existing claim refusal and its explicit limitation; do not silently
  implement scheduling to make tests pass.
- Continue in the existing Opus 5 / medium herdr session. Edit directly, leave
  changes uncommitted, do not launch agents or send external messages, and do
  not monitor other agents. Preserve unrelated work and existing user evidence.
  If a substantive conflict remains after removing obsolete interfaces, report
  it clearly rather than inventing a compatibility layer.

The first pass's green suite is evidence for that pass, not a reason to preserve
its interfaces. Report the final simplified API, removed surfaces, exact checks,
and genuine remaining limitations. Do not claim the whole study workflow is
complete merely because its intentionally narrower shared-store core works.
