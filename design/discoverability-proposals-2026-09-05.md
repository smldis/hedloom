# Discovering work and returning to its evidence

2026-09-05 · One-page proposal · Source baseline: `bfaac7d` after PR #19.
The [handoff](../DISCOVERABILITY-ATTEMPT-IDENTITY-HANDOFF.md) supplies suggestions;
the proposals below are options for discussion, not adopted contracts.

**Need and starting point.** An operator should be able to find yesterday's
work, inspect failures, and reach earlier results without remembering authored
keys or filesystem identities. Shared computation records and numbered tries
now exist independently of their consumers. Outcomes carry exact record/try
references, and `StudyRun.outputs` exposes named results, but the facade keeps
the study/run relationship only in memory. `latest/` and its query commands
have been removed. These are current source observations in
[study.py](../src/hedloom/study.py), [durability.py](../exec/src/hedloom_exec/durability.py),
and [reuse.py](../exec/src/hedloom_exec/reuse.py); no new runtime verification
was performed for this proposal.

**1. Computation-store browser — useful independently, smallest change.**
Proposed `hedloom attempts --site SITE --since DATE --outcome failed` lists
individual tries, newest first, with operation where recorded, timestamps,
state, and a copyable selector. `hedloom inspect --site SITE 'RECORD#TRY'`
shows recorded errors, placement, values, artifacts, diagnostics, and workspace
paths. Expand every record's tries rather than showing only its standing try.
Distinguish execution state, standing reusable evidence, pins, and payload
availability. Reading must not launch, reconcile, pin, or alter anything.
This works with existing readable records, including unfinished work, but cannot
recover which studies consumed them. It solves filesystem hunting; it only
partly solves returning to an engineering activity.

**2. Persistent run history — recommended core feature.** Proposed
`hedloom runs --site SITE --study NAME --since DATE` discovers submissions;
omitting the study searches all recorded runs. `hedloom run --site SITE RUN_ID`
@hedoom run sounds like we are launching something
opens a run's Plan, named outputs, invocation outcomes, and links into feature 1.
Give each submission its own ID; keep study names as searchable labels.
Record each invocation's selected record/try and execution/reuse disposition
when reported, so two studies can independently reference the same evidence.
Blocked or refused invocations retain their reason without invented references.

Persist the run header and Plan before execution and save progress as outcomes
arrive, outside reclaimable workspaces. A crash must leave visibly incomplete
history; an unreported outcome stays unknown even if a shared record later
succeeds. Publish completion only after the final account is saved, and surface
history-write failures separately from computation outcomes. This adds a small
persistence contract, but directly addresses "what did my submission do?"
Existing records without run history remain browsable through feature 1;
consumer history cannot be reconstructed from them reliably.
@i would add at least a way to get the journal or attempt directory of an invocation so ican then pipe the output to cd.

**3. Result navigator and run comparison — follow feature 2 when useful.**
Start from exported output names, show the producing invocation and exact try,
then traverse dependencies to their evidence. Compare two runs by authored keys
and output names, showing declaration changes, reused/shared references,
execution outcomes, and missing payloads. Renamed nodes remain unmatched unless
explicitly mapped; equal record identities establish shared computation, not
equivalent experimental intent. This helps explain a changed result, with more
matching and presentation work than the first two features.

**Engineering shape and recommendation.** Deliver features 1 and 2 as one useful
vertical slice; feature 1 can ship first. Put discovery/history in Hedloom,
consuming Exec's journal/manifest readers and Run's outcome references. Expose
structured Python queries plus CLI tables and `--json`; a GUI can consume those
later. Begin with file-backed history and scans, adding an index only if measured
latency warrants it. Pins with multiple requesters, study pinning, new retention
rules, and replacement mutable aliases are separate proposals, unnecessary for
this slice. Inspecting a verdict never implies accepting a conclusion.

**Evidence that would earn adoption.** In a fresh process, find and inspect a
failed submission without known keys; show two studies sharing one try while
retaining both histories; preserve an old run's reference after standing changes;
identify reclaimed payloads; and show an interrupted run as incomplete. These
checks should cover both kernels and value-only outputs as well as files.
