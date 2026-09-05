# Shared computation identity: implementation and verification

> Historical implementation brief, completed by PR #19 (merge `604f53f`).
> Preserved as the original scope and execution instructions, not a current task.
> Current behavior is documented in the maintained guides and ontolomes; remaining
> discovery and pinning work is summarized in the
> [updated handoff](../DISCOVERABILITY-ATTEMPT-IDENTITY-HANDOFF.md).

**Scope superseded after review:** the user explicitly authorized removal of
the old ownership interfaces and `latest/`, with no backward compatibility.
Continue from [the shared-store cleanup brief](shared-store-cleanup-2026-09-05.md).
The preservation constraints below describe the first pass, not current scope.

2026-09-05. Authorized implementation plan from the attempt-identity discussion.
The user requested a Claude Opus 5 agent at medium effort, launched through
herdr from `/home/smldis/working/AI`, to implement this bounded work.

## Contract being adopted

Within a shared record store, a declared computation digest selects one record,
independent of the requesting study name, authored invocation key, Plan ID,
placement, scheduler, or try number. Each actual execution receives a numbered
try under that record. Reuse selects existing evidence; another requesting
context does not itself require another execution.

Equality means equal declared computational dependencies, under the existing
author responsibility to declare them faithfully. It does not prove equality
of all causal dependencies, source immutability, or determinism automatically.
An intentional independent repetition must declare a computational distinction
such as a seed or repetition parameter. Merely naming a second invocation
differently does not request independent execution.

Keep the current identity-bearing bundle fields unless investigation exposes
authoring-only identifiers leaking into the digest indirectly. Source
addresses, declared output names/paths, operation identity/version/body
fingerprint, arguments, dependency digests, and identity environment can remain
meaningful computational declarations. This is not semantic code equivalence,
content-hashing every external resource, or hashing the entire Plan document.

## Scope and boundaries

Implement shared identity, its production derivation path, necessary direct
consumers, focused regressions, and current contract documentation. Preserve
record/try separation, placement independence, input-mismatch refusal, atomic
publication, claim exclusivity, and the existing source-fingerprint assumptions.

The following are explicitly deferred:

- Study/run history, discovery UI, and general many-consumer provenance.
- Multiple-requester pins and study pinning.
- Removal/replacement of `latest/`; leave its current behavior in place for
  this bounded identity pass, without strengthening its status as a contract.
- Scheduler coalescing, waiting on competing claims, cancellation policy,
  nested-study redesign, retry policy, and resource donation.
- Migration or compatibility with old stored identities; this prototype
  already permits identity changes without migration. Do not scan and delete
  existing stores or run against user evidence roots.

Scheduling follow-up is in
[the nested-study TODO](nested-studies-as-a-tree-2026-09-05.md#follow-up--shared-computation-requests-and-nested-scheduling-2026-09-05).
History and pin context is in
[the handoff](../DISCOVERABILITY-ATTEMPT-IDENTITY-HANDOFF.md).
These deferrals are real limitations: this pass does not promise every
simultaneous equivalent requester receives a successful shared result, or that
creator-based record discovery lists all later consumers.

## Implementation sequence

1. **Establish the current baseline.** Read applicable `AGENTS.md`/`CLAUDE.md`,
   manifesto, ontolomes, relevant README/unit definitions, and Exec's current
   decision ledger. Inspect Git status/diffs before editing. The historical
   discussion and saved skill measurements are not test results for this tree.
   Run the full four-directory test suite once and record pre-existing failures.

2. **Make identity itself computation-based.** Inspect
   `exec/src/hedloom_exec/identity.py`, `reuse.py`, `planned.py`, and
   `durability.py`, plus all callers. Render the record solely from the declared
   computation digest. Make missing/empty digest handling explicit: do not
   silently collapse digest-less requests onto one record or retain a hidden
   study/key hash fallback. Keep identity object equality consistent with record
   equality; requester metadata must not make one shared record compare as two
   identities. A clean prototype API change is allowed; update all callers and
   fixtures rather than inventing compatibility machinery.

3. **Integrate without absorbing study history or scheduling.** Keep study and
   invocation metadata available for request reporting and creator attribution,
   but exclude it from record derivation. Trace upstream reference normalization
   so renaming a producer does not invalidate equivalent downstream work. Inspect
   binding and both kernels for assumptions about one invocation per record;
   preserve one outcome per authored invocation even when sequential requests
   reuse the same execution. Keep the existing competing-claim refusal; do not
   suppress it, introduce automatic retries, or change Dask task keys to hide it.
   Check lineage and CLI consumers for false completeness claims under sharing.
   Document creator-only discovery limitations rather than implementing a second
   study-history system. If a direct consumer cannot remain honest without the
   deferred redesign, stop and report the concrete conflict.

4. **Update current contracts deliberately.** Cover Hedloom/Exec identity
   statements, Exec `DECISIONS.md`'s cross-plan reuse decision, and
   `docs/internals/mechanism.md` / `attempt-claim-protocol.md`. Update other
   maintained documentation that explicitly promises study/key-isolated records.
   Replace unconditional soundness claims with the declared-dependency
   assumptions. Explain creator attribution and unresolved concurrent reuse.
   Historical design notes remain historical. Preserve unrelated edits in
   shared documents. Update tests whose intended isolation depended solely on
   different study/key names; independent work should differ computationally.

5. **Verify and report.** Run the relevant regressions, the full suite, docs,
   and whitespace checks below. Review remaining identity references for hidden
   namespace assumptions. Report exact commands/results, changed files, known
   limitations, and any pre-existing failures. Do not commit or publish.

## Verification matrix

| Case | Required evidence |
| --- | --- |
| Study rename / separate studies, same declarations and store | Same record and sequential reuse; counted body/transport executes once. |
| Authored key rename / two equal invocations in one Plan | Same record; sequential run retains both distinct invocation outcomes and executes once. |
| Producer key rename in a dependency chain | Equivalent producer and downstream computation identities stay unchanged. |
| Different computational declarations | Different record: cover arguments, implementation/version, supplied source fingerprint, upstream declaration changes, and identity environment through the appropriate API. |
| Placement or kernel change | Same record and equivalent values in noncontending runs; reuse crosses sequential/graph execution where already supported. |
| Intentional repetition | Explicit differing seed/repetition argument gives distinct computation records. |
| Retry / prior failure | A nonreusable terminal failure produces a new numbered try in the same record; successful reuse selects existing evidence. |
| Missing digest / stale or unchecked identity | Production identity cannot silently omit declared computation; existing mismatch refusal and deliberate test escape hatches remain honest. |
| Competing claims | Deterministically hold one claim, show the equivalent request reaches the same record and is refused; no duplicate launch. This verifies exclusion, not completed result sharing. |
| Caller attribution / existing aliases | Sequential second callers keep their own invocation names and outcomes; shared-record creator metadata is not advertised as complete consumer history. Existing aliases still resolve for each caller pending their separate removal. |

Use existing meaningful tests where they already prove a row; add focused
integration regressions where the changed contract is not covered. Assert
actual launches and shared record/try evidence, not just equal hash strings.
Use temporary roots, local execution, and existing fake transports. No farm
jobs, external messages, or user artifact pruning are needed.

From `/home/smldis/working/AI/analog-sim-studies/hedloom`:

```sh
PYTHONPATH=src:flow/src:exec/src:run/src .venv/bin/python -m pytest -q tests exec/tests run/tests flow/tests
git diff --check
rg -n 'attempt_identity|plan_id|invocation_id|cross.plan reuse|cross.study reuse' src exec/src run/src flow/src docs exec/docs run/docs flow/docs ONTOLOME.md exec/ONTOLOME.md run/ONTOLOME.md flow/ONTOLOME.md exec/DECISIONS.md
```

From `/home/smldis/working/AI/analog-sim-studies`:

```sh
hedloom/.venv/bin/python composition.py docs
```

Read documentation warnings, not just the exit status. If the existing venv
lacks necessary dependencies, report the specific limitation before attempting
installation. Do not weaken assertions merely to obtain a green suite; distinguish
old identity expectations that must change from actual regressions.

## Shared-workspace safeguards

Other agents share this filesystem. Pre-existing modifications at planning
time include Hedloom `ONTOLOME.md`, `README.md`,
`docs/guide/authoring.md`, `docs/guide/results.md`, and `src/hedloom/study.py`.
Untracked `CLAUDE.md` guidance files and the discoverability handoff are also
present. This turn adds the nested scheduling note and this plan. Inspect again
at execution time: the list is not permission to overwrite later changes.

Prefer the narrow identity/Exec work and existing public seams. Before editing
a shared file, re-read its current contents and preserve unrelated changes.
Do not reset, clean, stash, move worktrees, stage, commit, push, alter global
agent settings, change model/effort, or launch additional agents. If this plan
proves wrong or impossible within its boundaries, stop and report the concrete
conflict; do not improvise a different design.
