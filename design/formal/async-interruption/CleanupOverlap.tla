-------------------------- MODULE CleanupOverlap --------------------------
EXTENDS Naturals
CONSTANTS MatchingCleanup, ProtectRunning, OrderedCleanup, RunningMessage
VARIABLE q
vars == <<q>>
\* Same protocol cut as CleanupOverlap683a88f, with distinct scheduler owner.
Init == q = [workerPaused |-> TRUE, schedulerPaused |-> TRUE,
 marker |-> "A", owner |-> "A", a |-> IF OrderedCleanup THEN "scheduler" ELSE "worker",
 b |-> "pause", message |-> RunningMessage, overwritten |-> FALSE]
AWorkerCleanup ==
 /\ q.a = "worker" /\ q.marker = "A"
 /\ q' = [q EXCEPT !.workerPaused = FALSE, !.marker = "",
                  !.a = IF OrderedCleanup THEN "done" ELSE "scheduler"]
BPause ==
 /\ q.b = "pause" /\ ~q.workerPaused /\ q.marker = ""
 /\ q' = [q EXCEPT !.workerPaused = TRUE, !.marker = "B", !.b = "prepare"]
BPrepare ==
 /\ q.b = "prepare"
 /\ q' = [q EXCEPT !.schedulerPaused = TRUE, !.owner = "B", !.b = "ack"]
ASchedulerCleanup ==
 /\ q.a = "scheduler"
 /\ LET permitted == ~MatchingCleanup \/ q.owner = "A" IN
    q' = [q EXCEPT !.schedulerPaused = IF permitted THEN FALSE ELSE @,
     !.owner = IF permitted THEN "" ELSE @,
     !.a = IF OrderedCleanup THEN "worker" ELSE "done",
     !.overwritten = @ \/ (permitted /\ q.owner = "B")]
\* A delayed duplicate cleanup may arrive after B's claim. Current finally
\* uses scheduler-first ordering; matching-owner checks defend this wider cut.
DelayedCleanup ==
 /\ q.a = "done" /\ q.owner = "B"
 /\ IF MatchingCleanup THEN UNCHANGED q
    ELSE q' = [q EXCEPT !.schedulerPaused = FALSE, !.owner = "",
                        !.overwritten = TRUE]
RunningNotification ==
 /\ q.message
 /\ q' = [q EXCEPT !.message = FALSE,
      !.schedulerPaused = IF ProtectRunning /\ q.owner # "" THEN @ ELSE FALSE,
      !.overwritten = @ \/ (~ProtectRunning /\ q.owner = "B")]
BAck ==
 /\ q.b = "ack"
 /\ q' = [q EXCEPT !.b = IF q.schedulerPaused THEN "released" ELSE "error"]
Next == AWorkerCleanup \/ BPause \/ BPrepare \/ ASchedulerCleanup
 \/ (RunningMessage /\ DelayedCleanup) \/ RunningNotification \/ BAck \/ UNCHANGED q
Spec == Init /\ [][Next]_vars
NoSchedulerPauseOverwrite == ~q.overwritten
WorkerPausePreserved == q.marker = "B" => q.workerPaused
NoSpuriousAckError == q.b # "error"
=============================================================================
