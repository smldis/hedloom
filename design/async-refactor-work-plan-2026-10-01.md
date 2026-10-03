# Async refactor: implementation work plan

User authorization, 2026-10-01: reconsider the earlier detailed proposal using
engineering judgment and carry out the complete refactor, maintained callers,
documentation and tests autonomously. Earlier report-only scope is superseded.
Preserve the original design records and ovnotes; no publication is requested.

## Chosen implementation scope

- One Runtime-owned daemon controller thread/asyncio loop per explicitly created
  Runtime. Dask remains the ready-work executor, using existing placement caps.
  No per-Run controller threads, sequential execution path, or worker-held nesting.
- Immediate Run receipts; separately observable durable acceptance and terminal
  results. Explicit waiting is an observation operation. Completion and success
  are distinct. Context exit drains normally and withdraws on exceptional exit.
  Process exit reclaims owned resources automatically; abrupt exit may leave
  incomplete evidence and cannot promise graceful terminal history.
- Waiting Runs have no arbitrary default count/Plan cap. Bound concurrent owned
  offload and submitted execution work; retain waiting Plans as data. No new
  runtime-limit TOML surface, persistence service, migration engine, aging
  scheduler, or generic executor/plugin layer.
- Submission priority defaults to zero and orders eligible controller/Dask/pool
  work. Equal priority uses dispatch round-robin; execution windows equal capacity.
  Priority is excluded from computation identity and does not imply preemption,
  an LSF priority, or live reprioritization of dispatched Futures.
- Preserve shared exact Exec references, late artifact identity, durable links
  before dispatch, and withdrawal of one consumer without cancelling another.
- Capture the selected operation functions at registration and prepare their
  serialization/source evidence off-loop. Do not claim hermetic snapshots of
  arbitrary mutable external state.
- Retire blocking Session/Study.submit/top-level submit/submit_all and low-level
  synchronous plan runners. New public Runtime.submit returns Run; Run.wait and
  result are explicit observation methods. Keep authoring/Plan/output/Exec semantics.
- History schema 4 only, with lifecycle metadata; no schema-3 compatibility or
  migration. Saved user data is untouched. Preserve records_dir/runs_dir/work_dir.
- Preserve established farm-compatible dependency policy, testing the async path
  in the ASS environment and the older supported Dask/distributed environments
  where available. Use isolated fake LSF, not real-farm jobs.
- Convert current nested consumers to explicit caller staging without assuming
  this is a permanent answer to future hierarchical composition use cases.

## Work ownership

The containing ASS checkout is the shared workspace. Root owns public Runtime,
Run receipts/results, body capture/history, exports, facade tests, docs, final
integration and review. Controller agent owns Run readiness/sharing/withdrawal,
execution helpers, relevant tests and Run ontology. Executor agent owns async
cluster/pool integration and resource refusal. Consumer agent owns examples and
ASS study callers. Assignments exclude one another's files; coordinate interface
changes explicitly and preserve unrelated work.

## Checkpoints

- [x] New controller and facade run one real recorded operation end to end.
- [x] Staggered Runs, priorities, shared consumers, failures and withdrawal pass.
- [x] Automatic process exit and optional/context shutdown pass bounded probes.
- [x] Direct and pooled fake-farm operation/startup/teardown pass.
- [x] Maintained callers use the new surface; no retired runtime APIs remain.
- [x] Reload/source capture, history/discovery/reuse and retention checks pass.
- [x] Maintained docs/ontologies/configuration describe implemented contracts.
- [x] Unit/integration suites, compatibility checks and composed docs reviewed.

Update these checkpoints and concise evidence during implementation. Unexpected
friction may change the mechanism; record consequential adopted changes and
their verified limits rather than defending the earlier proposal.

## Final evidence, 2026-10-02 (work began 2026-10-01)

The combined facade, Exec, Run, Flow and ASS integration run passed 798 tests,
with two skips, on Python 3.11.16 / Dask and distributed 2026.7.1. Subsequent
history/lifecycle checks passed 73 tests after the final storage exception fix;
a further registration/helper-rebinding test passed. Together these cover 800
unique passing cases and two skipped cases in the final source. The warnings
in the combined run came from deliberately injected Exec selection publication
failures. Real local ngspice exercised the staged OTA discovery/corners/report
workflow, saved Plan, exact references and reuse.

The full facade and Run suites passed 323 tests with two skips on each matching
cached Dask/distributed 2023.9.2 and 2024.8.0 overlay. The final late-history and
helper-capture regressions then passed separately on both versions. No packages
were installed. The strict composed Sphinx build passed with `-E -W --keep-going`;
source/API retirement and whitespace checks passed.

Local Linux entered-command owner-death probes passed for ordinary exit,
SIGTERM and SIGKILL without manual close. Cold Jobqueue startup preserved the
caller's SIGINT handler; ordinary process exit reclaimed the fake pooled worker
and its entered command. Initial abrupt pooled SIGTERM/SIGKILL probes left work
alive after five seconds, and those probes explicitly reclaimed their jobs.
Extended 2026-10-02 probes observed automatic command and fake-worker cleanup in
32.45 seconds (SIGTERM) and 32.50 seconds (SIGKILL). Logs and installed Dask source
show immediate scheduler-loss detection followed by the default 30-second worker
executor shutdown grace. The initial check measured a delay, not permanent
orphaning. No runtime change was needed for the extended result. These remain
local fake-farm observations, not real-farm deadlines. No real farm jobs or
commits were made.

## Adopted consequences and remaining limits

- Manual close is optional, but creating a Runtime begins opening configured
  resources. Context/explicit close drains registered Runs; ordinary process exit
  has bounded cleanup and may leave incomplete history.
- Resource bounds apply to entered offload work and outstanding executions.
  Waiting Runs have no fixed count quota; retained Plans still consume memory.
- Priority orders pending eligible work without changing identity. It does not
  preempt, become farm priority, reprioritize dispatched Futures or prevent
  starvation under sustained higher-priority arrivals.
- The first cold pooled Runtime needs the main thread for Jobqueue import;
  local Runtime does not import Jobqueue. Warm pooled construction can occur on
  another thread. A cold background construction rejects startup inspectably.
- Body code and same-module helpers are pinned at registration. Arbitrary mutable
  external state is not frozen; preparation-time source bytes may differ from
  earlier selected code. Saved bindings include code hashes, not a code archive.
- Worker-held nesting is deferred. Maintained callers compose caller-owned stages
  without occupying a parent's executor slot while children need it.
- Consumer history writes schema 4 and refuses older schemas without deleting
  saved evidence. Plan schema and Exec record/identity/reuse contracts are preserved.

Original dated concepts, proposal and ovnotes are preserved. Maintained docs and
ontologies describe the resulting implementation; the component remains a prototype.

## Publication verification, 2026-10-02

The user subsequently requested GitHub PRs and a clean review workspace. The
final combined facade, Exec, Run, Flow and ASS integration suite, including the
force-stop additions, passed **841 tests with two skips** in 206.20 seconds.
Its two warnings were the expected injected selection-publication failure.
The strict composed Sphinx build passed again with `-E -W --keep-going`.
A real subprocess probe verified the corrected OTA dashboard browser launcher;
whitespace checks passed. Earlier compatibility checks remain scoped to their
recorded suites and targeted cases, rather than this entire final combined run.

Forced withdrawal now escalates ordinary stop for solely owned pooled commands
through targeted nanny restart within the same allocation. Local/fake-farm
tests cover ownership, queued collateral work, concurrent restart requests,
commands ignoring SIGTERM, exact cancelled history, retry and later successful
reuse. Other placements/Python bodies drain, detached descendants remain outside
immediate-child binding, and unexpected pool-worker loss can replay the separate
command task. Maintained guides and ontologies state these limits. No real-farm
jobs or package installations were needed for publication verification.
