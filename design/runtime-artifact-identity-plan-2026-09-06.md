# Runtime-identified outputs and fresh operations

Date: 2026-09-06. Status: feature design and implementation handoff;
implementation has not started.

Code inspected at Hedloom commit d03eb45c98348321c202e9d1dbc942e86d38e2b2.
The only workspace addition from this task is this plan. Findings come from
reading current code, contracts and tests; no runtime probes or tests were run.
This is the working review draft, revised with the user's clarifications.

## Resume prompt after compaction

The latest user request was to add the agreed development steps to this document
so it can be used as an implementation prompt after compaction. This preparation
turn changes documentation only. When asked to implement from this document, use
the following brief:

> Implement runtime-identified outputs and fresh operations according to this
> document. Start with the repository-checkout sequential vertical slice in
> section 7, then complete shared execution, content-identified files, and final
> integration. Use API A and named return mappings. Keep producer-derived identity
> as the economical default; use explicit content or declared identity where
> requested. Extend the existing ExecutionOwner bookkeeping, and keep reclamation
> in the existing collector. Record per-consumer requested inputs in history and
> actual executed inputs in the try journal. No compatibility or migration work
> is required. Do not restart the architectural review or implement alternative B
> alongside A. Resolve routine implementation details through the vertical slice
> and its tests; report any concrete evidence requiring a design change.

Working directory: /home/smldis/working/AI. Repository:
/home/smldis/working/AI/analog-sim-studies, with Hedloom under hedloom/.
Load the explicitly requested hedloom-dev skill and its saved context if this is
a fresh conversation, then read current applicable AGENTS.md, manifesto and
ontolomes. The saved skill snapshot is historical. Check current Git status and
relevant code before edits; another agent or the user may have advanced them.
Preserve unrelated changes and existing runtime evidence. Preserve inherited
agent configuration; do not launch agents, send external messages, or commit
unless newly instructed to do so.

At handoff, the only task-created file is this untracked document. No feature
code, new tests, probes, or commits have been produced. The checks performed were
document link and whitespace checks, not evidence that the feature works.

## Settled direction

- Acquisition is an ordinary operation, with ordinary execution outcomes.
- Fresh acquisition can be required per submission, including when downstream
  work eventually reuses. A moving request such as "latest" resolves again.
- The identity returned for a repository checkout describes the resolved
  revision. It is separate from the physical checkout path.
- External directories remain owned by their external manager even when an
  operation changes their checkout. Hedloom does not copy, delete, freeze or
  maintain their contents.
- The author guarantees that a borrowed location represents its declared identity
  and stays suitable while dependents need it. Fetch timeouts and expense are
  author decisions. Freshness is an execution obligation, not proof of remote
  freshness.
- Independent observations have separate computation records. Tries remain
  attempts/recovery within a record; they are not repurposed as observations.
- Keep economical producer-derived identity available as the normal choice.
  Explicit runtime identity is useful at selected output boundaries.
- Named return mappings are accepted by the user.
- Use the existing execution bookkeeping and collector as starting points.
  Do not create a second sharing or reclamation system for pulled data.
- No old-version support, migrations, old cache identities or old behavioral
  expectations constrain development. Keeping a mechanism is justified by its
  usefulness, not by compatibility.

This supersedes the mechanisms recommended by the earlier draft of this plan:
universal output hashing and moving shared execution into blocking Exec claims
are not recommended. The original [source-refresh todo](../todo/refreshed-sources-2026-09-05.md)
remains unmodified pending adoption of a replacement direction.

## 1. Current shared-execution bookkeeping

The table the user recalled is real:
[ExecutionOwner.groups](../run/src/hedloom_run/execution.py) maps a bound-graph
contract to a list of OwnedExecution entries. Each entry carries an
ExecutionHandle, future, and consumer set. Session owns this table.

[graph._run_owned_graph](../run/src/hedloom_run/graph.py) does the following
under the owner lock:

1. Looks up the entire graph using _graph_contract.
2. Persists each consumer's invocation-to-handle binding.
3. Registers consumers and submits entries that do not already have a future.

_graph_contract includes declarations, dependencies, policies, transport
configuration, roots and bindings. It is deliberately stricter than computation
identity. ExecutionHandle.enter guards dispatch against replay. Withdrawal
removes a consumer without cancelling execution another consumer needs.
Completed predecessors remain available with their active graph. Fully terminal
graphs are not a reusable result cache; Exec supplies completed-result reuse.

[Shared-execution tests](../tests/test_shared_execution.py) cover staggered
arrivals, shared failures, different stop policies, differing storage bindings,
withdrawal, cancellation before entry and replay refusal. These tests were
inspected, not rerun.

### Recommended extension: lookup a resolved invocation in the same owner

Change the unit of lookup from an entire graph to one ready, resolved invocation.
Retain the existing owner, entry/handle lifetime, consumer bookkeeping and durable
entry gate. Do not add a separate worker-side table or blocking-claim mechanism
to solve the same-session case.

Conceptual entry:

    owner.executions[sharing_key] = {
        handle,
        future_or_completion,
        consumers,
    }

Conceptual key:

    sharing_key = (
        record_store,
        finalized_computation_digest,
        execution_compatibility,
    )

Fresh operations also have a unique observation identifier in their computation
digest, so independent fresh calls cannot join. Normal consumers of their
runtime-identified outputs can converge after the outputs arrive:

    submission A: pull P1 -> commit C --+
                                      +-> one compatible analysis execution
    submission B: pull P2 -> commit C --+

P1 and P2 have distinct records. Both submissions retain their own input bindings
and invocation names while referring to the shared analysis handle.

Keep execution compatibility explicit: implementation binding, placement/resource
request, transports, record/workspace roots and relevant execution environment
must permit one execution to satisfy both requests. Do not blindly replace the
existing conservative binding comparison with a digest-only lookup.

For a runtime-identity input, equivalent validated artifact identities may have
different paths. The proposed contract allows one execution to read the first
admitted suitable path and satisfy both consumers. Path spelling is not part of
that artifact's meaning; an operation depending on it must declare that dependency.
Persist the actual selected path and distinguish it from each consumer's candidate
path. No general alternate-path fallback is implied if the chosen execution fails.
Other incompatible bindings do not join merely because the computation digest
matches. Independent/incompatible callers retain Exec's explicit record-claim
refusal; cross-Session joining is not part of this feature.

### The actual structural change

This is more than changing a dictionary key. The table is controller-owned;
a serialized worker copy would not coordinate the Session.

Recommend a controller-side admission loop: when predecessor futures complete,
resolve the invocation's input artifact identities and access values, finalize
its request, acquire/join the owner entry, persist the consumer binding, and
submit only if no compatible execution exists. Dask still executes admitted work
and enforces placement resources. The authored graph stays fixed; Run performs
dependency-based admission so lookup occurs when identity is known.

Use this same owner entry protocol for concurrent submissions using the
sequential kernel, with an in-process completion object rather than requiring
Dask. A sequential traversal still executes at most one invocation at a time;
joining another ready execution must not create another call to Exec.

A joining consumer waits on the existing completion in its controller, not in
an extra worker task occupying a placement slot. Never hold the owner lock while
waiting or executing a body.

Consumer identity must include the invocation occurrence, not just the submission,
because two invocations in one submission can share an entry. Outcomes and output
references are projected separately for each invocation; a future key cannot
assume it belongs to only one authored node.

Per-submission result maps retain completed predecessor artifacts. A late lookup
of a completed entry can create a new dispatch and let Exec choose completed
reuse or a new try after failure. The owner need not become a second completed
result cache. Existing consumers of the old entry retain its exact outcome and
selection. Replacing an entry must not let old release callbacks remove its
successor.

Admission ordering becomes: save the complete Plan before any work, then save
each resolved consumer binding before admitting that invocation. The current
requirement to bind all execution handles before starting any graph node cannot
remain literal when downstream sharing is resolved later. Binding failure blocks
that admission and is accounted for; it does not erase already executed work.
Exec selection publication remains diagnostic and never controls the attempt.

Retain useful cancellation semantics: withdraw only this consumer; prevent
unentered execution when no consumer remains; await entered execution rather
than pretending it never ran. Active shared failure is an actual outcome for
its subscribed consumers. A later independent dispatch follows Exec's ordinary
failure/retry rules.

## 2. Tentative output API

Two reasonable spellings exist. Implement one, not both.

### A. An identity option on the output declaration — recommended

    @operation(
        execution="each_submission",
        config={"repository": parameter(str), "ref": parameter(str)},
        outputs={
            "repo": directory(
                kind="repository",
                external=True,
                identity="declared",
            ),
        },
    )
    def checkout(*, repository, ref):
        path, commit = checkout_repository(repository, ref)
        return {
            "repo": located(
                path,
                identity={"repository": repository, "commit": commit},
            ),
        }

    @operation(
        inputs={"repo": artifact("repository")},
        outputs={"report": returned(kind="analysis-report")},
    )
    def analyse(repo):
        return {"report": analyse_repository(repo)}

    @study(name="repository-analysis")
    def analysis(repository):
        checked = checkout(repository=repository, ref="latest")
        return {"report": analyse(checked.repo).report}

checkout_repository and analyse_repository are author code, not Hedloom Git
features. Downstream bodies receive the selected access value (here a path),
not a live identity wrapper.

For an owned download:

    @operation(
        execution="each_submission",
        outputs={
            "document": file(
                "document.txt",
                kind="document",
                identity="content",
            ),
        },
    )
    def download(out):
        out.document.write_bytes(fetch_document())

Modes:

| Output identity | Meaning |
| --- | --- |
| producer — default | Use the producer's finalized computation digest and output name; no payload hashing. |
| content | Fingerprint the produced output, independent of the producer's execution and code. |
| declared | Use the canonical identity supplied with that named output. |

Ownership and identity are distinct. Workspace-relative file/directory declarations
are owned outputs. external=True requests a location supplied by the body; it
does not grant Hedloom ownership. Initially require declared identity for borrowed
outputs. That covers the agreed use case without implying content traversal of
externally managed trees. Further combinations can be added when needed.

An owned declared-identity output can use located(out.name, identity=...) and
must match the declared workspace path. Borrowed output must supply a valid
absolute location, normalized on the executing host. Wrong shape, missing
identity, noncanonical identity data, or a missing promised output fails explicitly.

Advantages: ownership and equality are visible beside the output's kind and
location; it extends the existing file/directory/returned vocabulary. Cost: the
constructors gain options and validation of allowed combinations.

### B. Wrap an output declaration with its identity rule

The same semantics can instead be spelled:

    outputs={
        "document": by_content(file("document.txt", kind="document")),
        "repo": by_identity(directory(kind="repository", external=True)),
    }

Unwrapped declarations retain producer identity. The runtime return still uses
located(path, identity=...). Wrappers can apply uniformly to file, directory,
stream and returned-value declarations without each constructor carrying the
identity argument.

Advantages: keeps storage declarations small and makes identity an explicitly
composable policy. Cost: more public helper names and nested expressions.
Recommend A for the present small API; neither changes the execution design.

### Named returns and initial support

The user accepted named return mappings. Returned values and declared identities/
borrowed locations are supplied under their declared output names:

    return {"report": report, "repo": located(path, identity=revision)}

Owned files are captured from their declared paths; a body writing only those
may return None. Shell remains the launch descriptor for ordinary declared
filesystem/stream outputs. Returning Shell does not magically supply a borrowed
location; a checkout body may run its author code and return located directly.

Remove output_value's fallback that treats the whole return as any missing port.
A required output missing its named value is an error. Update repository example
and study bodies to the named-return protocol; no legacy return adapter is needed.

The first content-identity vertical slice fingerprints files in full, using
streamed reads with no 64 MiB fallback. Normal intermediate files/directories and
returned values keep producer identity and require no new hashing. General
content identity for directories/JSON/streams needs defined canonicalization;
either implement that explicitly as a later extension or refuse unsupported
combinations in the initial API. Do not silently use size/mtime or object repr.

## 3. Suggested provenance placement

Use existing responsibilities and references rather than a parallel provenance
database.

| Location | What it records | When |
| --- | --- | --- |
| Submission history, per invocation binding | This consumer's resolved input identities, candidate paths/values, concrete producer record/try/output references, and chosen execution handle. | When inputs resolve, before joining/admitting execution. |
| Exec try journal | The actual bound input identities and producer/path references used by that execution. No study ownership fields. | A durable inputs_bound event before submit_intent and external launch. |
| Exec terminal manifest | Captured output identities, ownership/access data and outcome. | After capture, before the terminal journal event. |
| Existing execution handle | Exec's selected record/try and workspace, through selection.json and workspace.json. | Published by Exec selection, as today. |

The consumer binding can be one immutable document extending the existing
HistoryWriter.bind_execution payload, so inputs and the selected handle are not
two independently published claims. A shared handle's input choice is the first
admitted execution's choice; later consumers do not overwrite it.

Do not include provenance addresses or producer references in a declared/content
artifact's reuse key. The key reflects the asserted equivalence; the references
explain the physical inputs considered or used.

Example:

    Run A: P1 produces revision C at path X; analysis E executes using P1/X.
    Run B: P2 produces revision C at path Y; analysis selects E.

Run B's history records P2/Y. E's original try journal records P1/X. Its manifest
is never rewritten to claim it executed on P2/Y. The same distinction holds when
Run B joins E while E is active.

Store output identity once in the producer manifest. Downstream input evidence
can contain the selected identity plus its exact producer reference, making the
reuse decision inspectable even without retaining or opening the original payload.
The input event is authoritative execution evidence; a reused request does not
append a fictitious new inputs_bound event to an old try. Attachment/recovery
must retain the existing try's original input binding.

For a source declared through input_artifact, record its declaration/resolved
location and runtime fingerprint instead of inventing a producing try.

Discovery should expose historical identity and present accessibility separately.
An existing borrowed directory does not prove that it still contains the recorded
commit. No checkout verification or restoration is implied.

## 4. Identity calculation and changes to execution

The current input_digest function already hashes canonical bundle data. No new
hash algorithm is required merely to hash an artifact reference. The principal
change is what fills the inputs field and when that information becomes available.

In [planned.py](../exec/src/hedloom_exec/planned.py), _reference_identity currently
constructs output:<producer-digest>:<name>. plan_bundles derives every digest
upfront. Replace this with invocation specifications plus one Exec-owned finalizer:

    prepare_invocations(document) -> declarations and symbolic dependencies
    finalize_invocation(spec, resolved_artifacts, observation_id=None) -> bundle

The names are illustrative. Ordinary producer-mode references use the producer's
finalized digest and output name. Content/declared references use a tagged key
including artifact contract, representation and canonical identity. Input names
and collection order remain identity-bearing.

Resolve delivery using invocation/output references, not digest-shaped map keys.
Each resolved artifact carries identity, access value, and provenance. The
finalizer derives the consumer's computation key; Run supplies locations and
dependencies without implementing another digest algorithm.

Some ordinary descendants also need late finalization: their producer's identity
may itself depend on an earlier runtime-identified output. This is why adding
one callback to the pull operation is insufficient.

Use the fresh dispatch handle's persisted unique identifier as the observation
identifier. Include it only for operations requesting independent fresh execution;
do not inject it as a user function argument or into every downstream key.
Distinct calls get distinct observation records. Tries within each record retain
their normal failure/recovery purpose. A handle replay cannot mint another ID.
An invocation cancelled or blocked before entry does not perform an observation.

The Plan remains static. Outputs do not control branching, graph shape, placement
or the number of invocations. Identity is finalized before each invocation's own
execution; exact downstream identities need not exist before the whole study starts.
Blocked nodes with unresolved identities report no digest, rather than an invented
placeholder that looks like an executable record key.

## 5. Code disruption assessment

| Area | Change | Size/risk |
| --- | --- | --- |
| [Flow authoring](../flow/src/hedloom_flow/authoring.py), [model](../flow/src/hedloom_flow/model.py) | Execution mode, output identity/ownership declarations, validation and serialization. | Medium |
| [Exec planning](../exec/src/hedloom_exec/planned.py), [reuse](../exec/src/hedloom_exec/reuse.py) | Separate symbolic dependencies from finalized input identities; add explicit observation request identity. | High, concentrated in derivation rather than the hash algorithm |
| [Artifact capture](../exec/src/hedloom_exec/artifacts.py) | Runtime identity and access/provenance descriptors; full file hashing; explicit borrowed paths; named values. | Medium |
| [Attempt](../exec/src/hedloom_exec/attempt.py), [durability](../exec/src/hedloom_exec/durability.py), [journal](../exec/src/hedloom_exec/journal.py) | Persist actual bound inputs; validate identities before success; do not turn cached reuse into a new execution event. | Medium |
| [Run binding](../run/src/hedloom_run/binding.py) | Structured resolved artifacts, reference-based delivery and the shared finalizer. | High |
| [Graph](../run/src/hedloom_run/graph.py), [driver](../run/src/hedloom_run/driver.py) | Ready-invocation lookup/admission; per-consumer result projection and honest unresolved outcomes. | High |
| [ExecutionOwner](../run/src/hedloom_run/execution.py) | Adapt existing groups bookkeeping to resolved entries; retain consumer/withdrawal/entry protection; completion handling for both kernels. | High, highest concurrency uncertainty |
| [Body transport](../src/hedloom/binding.py), [public exports](../src/hedloom/__init__.py) | located normalization and named returns; ordinary paths still reach bodies. | Medium |
| [History](../src/hedloom/history.py), [discovery](../src/hedloom/discovery.py), [study outputs](../src/hedloom/study.py) | Resolved consumer binding, input provenance queries, runtime output metadata and named projection. | Medium |
| [Collector](../exec/src/hedloom_exec/prune.py), [pins](../exec/src/hedloom_exec/pins.py) | Apply existing ownership boundaries; no borrowed payload traversal/deletion/freezing. Test integration. | Small for this feature; general cache eviction is separate |
| Docs, examples, root studies/integration references | Named return updates, new contracts and fresh-input examples. | Broad but mostly mechanical |

Current publication order remains useful: submission intent is durable before
transport, and output capture/manifest publication precedes terminal journal
publication. The proposed history extension does not erase these distinctions.

Break formats where this model needs it. Use one new Plan schema and explicit
versions for changed evidence formats. No old readers, migration, fallback return
protocols or cache-key preservation work is required. Do not change the hash
algorithm or record format just to demonstrate that breaking changes are allowed.
Keep no duplicate semantic path solely for compatibility.

## 6. Reclamation and borrowed ownership

A pull owns an ordinary record, try and workspace. Owned downloaded files use
the existing collector. A borrowed checkout remains externally owned even if
the operation changed its revision. Pinning/pruning the pull workspace must not
modify or remove the referenced checkout.

Current collector scope must remain explicit: it reclaims eligible non-standing
try workspaces, retains journals/manifests, and protects standing reusable results,
pins and unfinished/unreconciled tries. It does not reclaim whole studies or
generally evict successful computations. This limitation applies to ordinary and
fresh operations alike; it is not a new storage mechanism needed by sources.

Use that collector for this feature. Do not repurpose tries as observations to
fit its policy. General reclamation of unpinned successful payloads can extend
the same collector, with reuse availability and active-consumer protection
addressed together. This plan does not silently promise that broader cleanup.

Borrowed reusable outputs rely on the author's lifetime guarantee; selecting one
should check the promised path shape/accessibility, not claim that path existence
verifies its revision. Use fresh execution for repointable/latest checkouts.
A historical path can become unavailable or denote another revision without
changing what the old journal records. Hedloom does not restore it automatically.

## 7. Agreed four-step development sequence

Follow these as reviewable milestones within one implementation task. Once
implementation is requested, proceed through passing milestones without treating
each as a separate approval gate. Keep progress and actual evidence visible.

### Step 1 — Repository checkout, sequential execution

Build the smallest runnable vertical slice through Flow, Exec, Run and the facade:

- Add API A's execution/output declarations, located data, named returns, and
  Plan serialization/validation needed for declared-identity borrowed outputs.
- Separate symbolic input references from runtime identity finalization. Reuse
  the existing digest calculation where appropriate; changing its inputs and
  timing does not itself require changing the hash algorithm.
- Give independent acquisitions separate observation records and normal tries.
- Deliver the borrowed directory path to the consumer without copying or hashing
  it, while using repository/revision identity for reuse.
- Record the consumer's resolved input binding and the actual execution's input
  evidence, with exact record/try references on reuse.

Use a temporary local Git repository as the fixture, not a live external checkout.
Resolve "latest" through A -> A -> B -> A: every acquisition executes, analysis
reuses for equal/resumed identity, and history distinguishes each acquisition
from the execution supplying reused evidence. Also verify missing/invalid
outputs and acquisition failure blocking dependents.

Completion evidence: a runnable sequential test using the public API and exact
history/journal assertions. This is the first implementation checkpoint, not
completion of the whole feature. Unsupported modes must refuse explicitly until
their later step is implemented.

### Step 2 — Shared execution

Adapt the existing ExecutionOwner table and admission path to ready invocations,
using the same finalizer developed in step 1. Retain useful entry/replay and
consumer-withdrawal protections; do not add a separate worker-side sharing table
or use blocking Exec claims as the same-session sharing mechanism.

Prove independent acquisitions can resolve to the same artifact and converge on
one compatible analysis execution. Check incompatible bindings, per-invocation
report projection, late arrivals, shared failures, and concurrent sequential
submissions as well as the graph kernel.

Completion evidence: targeted concurrency tests for withdrawal, cancellation
before/after entry, replay refusal, table-entry replacement and minimum-capacity
progress. This is the highest-risk milestone; use its evidence before broad
consumer/API cleanup. Nested submissions still need capacity reasoning. Do not
introduce worker tasks merely to wait for another shared execution.

### Step 3 — Content-identified files

Add identity="content" for owned file outputs through the same runtime-identity
path. Hash the full file using streamed reads, without a size/mtime fallback.
Use an ordinary acquisition operation that writes its output through out.

Completion evidence: unchanged bytes reuse downstream work; changed bytes change
identity, including changes preserving size/mtime; files above 64 MiB obey the
same rule. Ordinary producer-identity outputs remain economical and are not
silently hashed. Run these cases through both supported kernels.

### Step 4 — Complete integration

- Update repository-owned examples and studies to named returns. Rewrite the
  live-source example around an ordinary operation and runtime output identity.
- Complete public output/discovery behavior and provenance inspection without
  requiring the original author module or a live scheduler.
- Verify that pinning/pruning owned workspaces cannot delete, freeze or traverse
  borrowed payloads. Use the existing collector; broader eviction of successful
  results is outside this feature.
- Update affected ontolomes, the Exec decision ledger and maintained docs to
  the implemented contracts. Remove obsolete API explanations rather than
  adding compatibility machinery.
- Run all four unit test directories and affected composition integration checks;
  build published documentation and inspect warnings. Review the final diff.

From Hedloom, the complete unit-test selection is:

    PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q tests exec/tests run/tests flow/tests

Use the project's appropriate current Python environment. From the composition
root, build docs with python composition.py docs and use its current integration
test workflow. Run git diff --check. Do not infer coverage from a bare pytest
invocation that collects only the nearest unit.

Completion evidence: all section 8 acceptance cases are covered, relevant checks
pass, documentation matches the implementation, and limitations are stated from
actual evidence. Report the final API, changes, checks and remaining limitations.
Do not claim farm verification from local/fake tests.

Across every step, keep Exec independent of Flow and Dask. Flow holds declarations;
Exec owns identity/capture; Run owns binding/admission; the facade owns consumer
history and body adaptation. Break formats as needed without migrations. Preserve
unrelated changes and existing runtime data throughout.

## 8. Acceptance cases

1. Each fresh invocation runs for each submission; two consumers of one fresh
   invocation use one acquisition. Two separately authored fresh invocations
   remain two observations.
2. Latest resolves A -> A -> B -> A: pulls always execute; analysis reuses for
   unchanged/resumed artifact identity, and changes for B.
3. Same declared repository/revision at different suitable paths has equal
   artifact identity. A changed revision at one path changes it.
4. Producer-mode outputs retain economical conservative invalidation. A content/
   declared output stops acquisition-code/observation changes from propagating
   when its identity is equal. A changed consumer body changes its own identity.
5. Multiple named outputs are projected correctly; missing values fail. Only
   consumers of a changed runtime-identified output are invalidated; collections
   preserve member order. Unrequested payloads are not automatically hashed.
6. Full-file identity works above 64 MiB, ignores mtime changes, and detects
   changed bytes with unchanged size/mtime.
7. Acquisition failure, missing location or invalid identity blocks dependents
   without selecting an older acquisition as fallback.
8. Within a Session, independently resolved equal inputs join one compatible
   active execution. Different identities, roots or incompatible execution
   requirements do not join. Different candidate paths under an explicit
   equivalence contract retain accurate per-consumer and actual-execution evidence.
9. Multiple invocations sharing one completion get their own reports. A late
   consumer and a completing/replaced entry cannot lose references or cancel
   another generation. Shared active failures remain failures, not false reuse.
10. Cancellation before entry prevents work; withdrawal leaves remaining
    consumers intact; a last consumer awaits entered work. Reentry cannot call
    Exec again. Check one-slot progress and concurrent sequential submissions.
11. Run B's new producer and a reused/shared execution's original producer are
    both discoverable. The old try journal is not rewritten on reuse. Input
    evidence is durable before actual launch; manifests contain output identities.
12. Borrowed payloads survive workspace pin/prune unchanged. Existing standing
    protection still applies consistently; history can show inaccessible old
    paths without rewriting execution success or asserting the old revision is
    currently present.

During development, run focused tests for each milestone, then all four unit
test directories plus affected composition integration tests. Build docs and
inspect warnings. Use local Git fixtures for moving revisions and ordinary
temporary files for content tests; no external repository or farm is needed
for initial acceptance. No tests have been run for this planning pass.

## Handoff outcome

Use API A, named returns, the existing owner table adapted at invocation readiness,
and the provenance placement in section 3 as the implementation direction. API B
is retained only as an evaluated alternative. The user asked to put the four-step
sequence into this document for use as a prompt after compaction.

Controller admission and table lifetime remain the main implementation experiment,
to be settled by step 2's tests. Universal hashing, blocking record claims, broad
successful-result eviction and a separate collector are not prerequisites.
Implementation has not begun in this conversation; this document is the handoff.
