----------------------------- MODULE StopAdmitting -----------------------------
(***************************************************************************)
(* A TLA+ model of `_stop_admitting` in `hedloom_run.graph`: what happens to *)
(* a sweep's outstanding work when the first invocation comes back failed.   *)
(*                                                                         *)
(* This is hedloom's protocol *over* Dask, not Dask. The scheduler appears  *)
(* here as three facts and nothing else: a task is queued, or it is running *)
(* a Python thread, or it is finished; a cancel removes a queued task and   *)
(* cannot touch a running one. Placement, resources, stealing and worker    *)
(* capacity are all absent on purpose -- unbounded parallelism is the worst *)
(* case for the race being checked, so assuming it is conservative.         *)
(*                                                                         *)
(* The decision `_stop_admitting` makes is a three-way split of everything  *)
(* outstanding:                                                            *)
(*                                                                         *)
(*   in_flight  -- has a live Python stack. Cannot be stopped, so it is     *)
(*                 waited for and its real outcome reported.                *)
(*   finished   -- already done, not yet consumed. Waited for too.          *)
(*   cancelled  -- everything else. Cancelled, and reported `blocked`.      *)
(*                                                                         *)
(* The split is computed from one `Client.call_stack()` snapshot, and the   *)
(* environment keeps moving while it is being acted on. That window is the  *)
(* subject of this model.                                                  *)
(*                                                                         *)
(* Three constants make the design decisions switchable:                    *)
(*                                                                         *)
(*   ExecutingTest    -- "call-stack" (shipped) or "assigned", which is what *)
(*                       `Client.processing()` would have answered. See R10  *)
(*                       in docs/dask-scheduling-rules.md.                   *)
(*   PreserveInFlight -- whether work with a live stack is waited for.       *)
(*   BlockedFromRecord -- whether `blocked` is decided from the stale        *)
(*                       snapshot (shipped) or from the durable journal.     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Tasks,             \* one per invocation; independent, as a sweep's points are
    ExecutingTest,     \* "call-stack" | "assigned"
    PreserveInFlight,  \* TRUE (shipped) | FALSE (mutation)
    BlockedFromRecord, \* FALSE (shipped) | TRUE: classify from the durable record
    OutcomeFromRecord  \* FALSE (shipped) | TRUE: read the outcome from it too

ASSUME ExecutingTest \in {"call-stack", "assigned"}

VARIABLES
    state,      \* state[t]: "queued" | "running" | "finished" | "cancelled"
    outcome,    \* outcome[t]: what the invocation produced once finished
    journaled,  \* journaled[t]: the payload reached the substrate and said so
    lost,       \* lost[t]: the future was cancelled, so it can no longer answer
    report,     \* report[t]: the line this run's report will carry
    pc,         \* the controller: the as_completed loop and what follows it
    doomed,     \* what `_stop_admitting` decided to cancel
    keep,       \* what it decided to wait for: finished + in_flight
    stopped     \* whether the stop decision has been taken

vars == <<state, outcome, journaled, lost, report, pc, doomed, keep, stopped>>

Outcomes  == {"succeeded", "failed"}
Reported(t) == report[t] # "none"
Outstanding == {t \in Tasks : ~Reported(t)}

(* `journaled` is what the durable record can answer afterwards. A task that *)
(* acquires a thread runs `_run_one` -> `execute` -> `launch_or_attach`,      *)
(* which appends `submit_intent` before the substrate is touched. Dask cannot *)
(* interrupt that thread, so starting is the point of no return and the two   *)
(* are modelled as one step.                                                  *)

TypeOK ==
    /\ state \in [Tasks -> {"queued", "running", "finished", "cancelled"}]
    /\ outcome \in [Tasks -> Outcomes \cup {"none"}]
    /\ journaled \in [Tasks -> BOOLEAN]
    /\ lost \in [Tasks -> BOOLEAN]
    /\ report \in [Tasks -> Outcomes \cup {"none", "blocked"}]
    /\ pc \in {"loop", "snapshot", "cancel", "collect", "done"}
    /\ doomed \subseteq Tasks
    /\ keep \subseteq Tasks
    /\ stopped \in BOOLEAN

Init ==
    /\ state     = [t \in Tasks |-> "queued"]
    /\ outcome   = [t \in Tasks |-> "none"]
    /\ journaled = [t \in Tasks |-> FALSE]
    /\ lost      = [t \in Tasks |-> FALSE]
    /\ report    = [t \in Tasks |-> "none"]
    /\ pc        = "loop"
    /\ doomed    = {}
    /\ keep      = {}
    /\ stopped   = FALSE

----------------------------------------------------------------------------
(* The environment: Dask, and the farm behind it. Both actions stay enabled  *)
(* while the controller is deciding, which is the whole point.               *)

(* A queued task acquires a worker thread. From here it cannot be stopped:   *)
(* `client.cancel(force=False)` does not interrupt a running Python thread,  *)
(* so its `bsub -I` runs to completion and its journal is published.         *)
Start(t) ==
    /\ state[t] = "queued"
    /\ state' = [state EXCEPT ![t] = "running"]
    /\ journaled' = [journaled EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<outcome, lost, report, pc, doomed, keep, stopped>>

Finish(t) ==
    /\ state[t] = "running"
    /\ \E o \in Outcomes :
        /\ outcome' = [outcome EXCEPT ![t] = o]
        /\ state' = [state EXCEPT ![t] = "finished"]
    /\ UNCHANGED <<journaled, lost, report, pc, doomed, keep, stopped>>

----------------------------------------------------------------------------
(* The controller: the `as_completed` loop in `run_plan_graph`.              *)

(* One future's result is consumed and reported. A non-succeeded outcome     *)
(* triggers the stop, once.                                                  *)
Consume(t) ==
    /\ pc = "loop"
    /\ state[t] = "finished"
    /\ ~Reported(t)
    /\ report' = [report EXCEPT ![t] = outcome[t]]
    /\ IF outcome[t] # "succeeded" /\ ~stopped
         THEN /\ pc' = "snapshot"
              /\ stopped' = TRUE
         ELSE /\ pc' = "loop"
              /\ UNCHANGED stopped
    /\ UNCHANGED <<state, outcome, journaled, lost, doomed, keep>>

LoopDone ==
    /\ pc = "loop"
    /\ \A t \in Tasks : Reported(t)
    /\ pc' = "done"
    /\ UNCHANGED <<state, outcome, journaled, lost, report, doomed, keep, stopped>>

(* `_stop_admitting`, first half: one snapshot, and the three-way split.     *)
(* `executing` is what `_executing_keys` answers. Under "assigned" it is     *)
(* what `Client.processing()` would have answered instead -- every task,     *)
(* because each carries a placement annotation and is assigned at once.      *)
Snapshot ==
    /\ pc = "snapshot"
    /\ LET executing ==
             IF ExecutingTest = "call-stack"
               THEN {t \in Outstanding : state[t] = "running"}
               ELSE {t \in Outstanding : state[t] \in {"queued", "running"}}
           inFlight  == IF PreserveInFlight THEN executing ELSE {}
           finished  == {t \in Outstanding : state[t] = "finished"} \ inFlight
       IN  /\ keep' = inFlight \cup finished
           /\ doomed' = Outstanding \ (inFlight \cup finished)
    /\ pc' = "cancel"
    /\ UNCHANGED <<state, outcome, journaled, lost, report, stopped>>

(* `_stop_admitting`, second half: `client.cancel(...)` and then the blocked *)
(* lines. A queued task really is cancelled. A task that acquired a thread   *)
(* since the snapshot is not, and cannot be -- the cancel returns success    *)
(* and the work runs on.                                                     *)
(*                                                                           *)
(* With BlockedFromRecord, the blocked lines are decided *after* the cancel   *)
(* from the durable record rather than from the snapshot: anything that       *)
(* reached the substrate is moved into `keep` and reported honestly.          *)
Cancel ==
    /\ pc = "cancel"
    /\ LET escaped == IF BlockedFromRecord
                        THEN {t \in doomed : journaled[t]}
                        ELSE {}
           blocked == doomed \ escaped
       IN  /\ state' = [t \in Tasks |->
                          IF t \in doomed /\ state[t] = "queued"
                            THEN "cancelled" ELSE state[t]]
           /\ report' = [t \in Tasks |->
                          IF t \in blocked THEN "blocked" ELSE report[t]]
           /\ keep' = keep \cup escaped
           /\ doomed' = blocked
           \* `client.cancel` discards the future either way. For a task that
           \* already holds a thread the work runs on regardless, so the client
           \* keeps no way to ask what it produced -- only the journal does.
           /\ lost' = [t \in Tasks |->
                        IF t \in doomed /\ state[t] = "running"
                          THEN TRUE ELSE lost[t]]
    /\ pc' = "collect"
    /\ UNCHANGED <<outcome, journaled, stopped>>

(* `_collect_preserved`: wait for what was kept and record its real outcome. *)
CollectOne(t) ==
    /\ pc = "collect"
    /\ t \in keep
    /\ ~Reported(t)
    /\ state[t] = "finished"
    \* `_collect_preserved` calls `future.result()`. A cancelled future raises
    \* CancelledError, which `_abnormal` turns into a failed line -- not the
    \* outcome the work actually had. Only the attempt record still knows that.
    /\ report' = [report EXCEPT ![t] =
                    IF lost[t] /\ ~OutcomeFromRecord THEN "failed" ELSE outcome[t]]
    /\ UNCHANGED <<state, outcome, journaled, lost, pc, doomed, keep, stopped>>

CollectDone ==
    /\ pc = "collect"
    /\ \A t \in keep : Reported(t)
    /\ pc' = "done"
    /\ UNCHANGED <<state, outcome, journaled, lost, report, doomed, keep, stopped>>

Next ==
    \/ \E t \in Tasks : Start(t) \/ Finish(t) \/ Consume(t) \/ CollectOne(t)
    \/ LoopDone \/ Snapshot \/ Cancel \/ CollectDone
    \* The run is over. Stuttering here keeps TLC's deadlock check meaningful:
    \* it then fires only where the controller is stuck with work unreported.
    \/ (pc = "done" /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----------------------------------------------------------------------------
(* Properties. *)

(* Every invocation gets a line. A sweep's report is in plan order and must  *)
(* be total, or the run says nothing about a point it was asked to run.      *)
ReportIsTotal ==
    (pc = "done") => \A t \in Tasks : Reported(t)

(* A line that says `blocked` is a claim that the work never happened. It is *)
(* what an operator reruns on, and what a reader of the report believes.     *)
BlockedNeverRan ==
    \A t \in Tasks : (report[t] = "blocked") => ~journaled[t]

(* Stopping admission means admission stops: once the cancel has been        *)
(* applied, nothing is left waiting to start.                                *)
NoQueuedWorkSurvivesTheStop ==
    (pc \in {"collect", "done"} /\ stopped)
        => \A t \in Tasks : state[t] # "queued"

(* A reported outcome is the outcome that invocation actually produced.      *)
TruthfulOutcomes ==
    \A t \in Tasks :
        (report[t] \in Outcomes) => (report[t] = outcome[t])

=============================================================================
