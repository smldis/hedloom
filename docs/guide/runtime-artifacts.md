# Fresh operations and runtime artifact identity

Acquisition is an ordinary operation. `execution="each_submission"` requires
each invocation to execute for every submission, even when its consumers reuse
earlier results. Freshness is an execution obligation; the author implements
fetching, timeouts, and any check that a remote service returned current data.

```python
from hedloom import operation, file, artifact, returned, study

@operation(execution="each_submission",
           outputs={"document": file("document.txt", kind="document", identity="content")})
def acquire(out):
    out.document.write_bytes(fetch_document())  # author-supplied acquisition

@operation(inputs={"document": artifact("document")},
           outputs={"report": returned()})
def analyse(document):
    return {"report": analyse_document(document)}

@study
def reading():
    return {"report": analyse(acquire().document).report}
```

The acquisition runs every time. Equal file bytes select the same analysis
computation; changed bytes select a new one. The runnable
[live-source example](../../examples/live_source.py) exercises this using a
local dictionary as its service and one worker slot.

For nested submission through the same live Session, see the separate
[nested-studies example](../../examples/nested_studies.py) and
[Session guidance](running.md#nested-studies-in-one-session). Fresh acquisition
no longer requires staging, but nesting remains available for integrating a
study submitted from an operation.

## Identity and ownership

| Declaration | Reuse identity | Payload work |
| --- | --- | --- |
| Default `identity="producer"` | Producer computation digest and output name | No content hashing |
| Owned or external file with `identity="content"` | Artifact contract, representation and full-file digest | Streams every byte, regardless of size or mtime |
| Filesystem output with `identity="declared"` | Artifact contract, representation and author's canonical JSON identity | Trusts the declaration |

Content identity supports owned and external files. Directory, stream and
returned-value content identity refuse. Ordinary intermediates keep the
economical producer default. A content/declared boundary can preserve downstream
reuse when acquisition code changes but its output identity stays equal.

An external file can use automatic content hashing without being copied:

```python
from hedloom import located

@operation(execution="each_submission",
           outputs={"document": file(kind="document", external=True,
                                      identity="content")})
def pull_document():
    return {"document": located(fetch_document_path())}  # author-supplied puller
```

Omit the workspace filename and the `located` identity: Exec hashes every byte
of the returned file at capture. Equal bytes allow downstream reuse even at a
different path; changed bytes invalidate consumers. The path remains borrowed:
Hedloom does not copy, freeze, restore, or delete it. The author must keep it
stable and accessible to consumers, including farm workers, while they use it.
Hashing captures identity; it does not enforce immutability. Referenced files
are separate dependencies and are not included in this file's digest.

Borrowed repository checkouts require no copying or hashing:

```python
from hedloom import directory, located, parameter

@operation(execution="each_submission",
           config={"repository": parameter(str), "ref": parameter(str)},
           outputs={"repo": directory(kind="repository", external=True,
                                      identity="declared")})
def checkout(repository, ref):
    path, commit = checkout_repository(repository, ref)  # author code
    return {"repo": located(path, identity={"repository": repository, "commit": commit})}
```

`ref="latest"` is the request; the resolved commit is the artifact identity.
Downstream bodies receive the path. Different suitable paths with the same
declared identity may satisfy the same consumer computation. Authors must declare
path spelling separately if it affects their result.

The external manager owns a borrowed path even when acquisition changes its
checkout. The author guarantees that it represents the declared revision and
remains suitable while dependents need it. Hedloom checks filesystem shape and
accessibility, but does not verify, restore, copy, freeze, or delete that payload.
For an owned declared-identity output, return `located(out.name, identity=...)`;
the location must match its workspace declaration.

## Named outputs and evidence

Return mappings use output names: `return {"report": report}`. Required missing
names fail. Bodies writing only declared owned files may return `None`; `shell`
still describes a launch for owned files or streams. It supplies no borrowed
location automatically.

Every independent fresh invocation gets a separate computation record, using
its durable dispatch identifier. Multiple consumers of one authored acquisition
share that observation. Tries remain attempts/recovery within a record.

The complete Plan is saved before work starts. As inputs resolve, each consumer's
history saves its candidate identities, paths/values, exact producer references,
and selected execution handle before joining or admitting execution. A Session's
existing owner table shares compatible active invocations after this resolution.
Compatibility includes implementation, placement, transport and storage bindings;
there is no cross-Session joining guarantee.

The actual try writes `inputs_bound` before submission intent and launch. Its
manifest records captured output identity and ownership before terminal
publication. Reuse does not append a fictitious input event or rewrite old input
evidence. If B acquires P2 at path Y but reuses analysis originally run on P1 at X,
B's requested inputs retain P2/Y and the analysis journal retains P1/X.

`RunHistory.invocation(...)` exposes `requested_inputs` and `executed_inputs`.
`RunHistory.outputs(...)` includes artifact metadata and `accessible` alongside
historical `available`; `StudyOutput.accessible` performs the same current access
check. A present directory does not establish that its recorded commit is still
checked out. Missing borrowed paths refuse reuse without restoring them.

Plan schema 4 and history schema 2 are the current formats. No migration or
legacy return protocol is provided. Runtime identity changes neither the static
graph nor placement. A blocked invocation whose inputs never resolved reports
no computation digest.

## Reclamation

Owned pull outputs use the existing workspace collector. Borrowed locations are
outside workspace pin/prune ownership. Standing successful results, pins, and
unfinished or unreconciled tries remain protected; eligible non-standing try
workspaces can be reclaimed while journals and manifests remain. General eviction
of successful results is outside this feature, for fresh and ordinary operations
alike. A pull introduces no separate collector.
