-------------------------- MODULE CleanupOverlap683a88f --------------------------
EXTENDS Naturals
VARIABLE q
vars == <<q>>
\* A has timed out after scheduler withdrawal but before restart. Its finally
\* block is about to call worker cleanup. B is still locally queued there.
\* This is a reachable protocol cut, not the PoolInterrupt initial state.
Init == q = [workerPaused |-> TRUE, schedulerPaused |-> TRUE,
 marker |-> "A", a |-> "worker", b |-> "pause", overwritten |-> FALSE]
AWorkerCleanup ==
 /\ q.a = "worker" /\ q.marker = "A"
 /\ q' = [q EXCEPT !.workerPaused = FALSE, !.marker = "", !.a = "scheduler"]
BPause ==
 /\ q.b = "pause" /\ ~q.workerPaused /\ q.marker = ""
 /\ q' = [q EXCEPT !.workerPaused = TRUE, !.marker = "B", !.b = "prepare"]
BPrepare ==
 /\ q.b = "prepare"
 /\ q' = [q EXCEPT !.schedulerPaused = TRUE, !.b = "ack"]
\* _resume_interrupted_worker has no ownership token. It checks only status.
ASchedulerCleanup ==
 /\ q.a = "scheduler"
 /\ q' = [q EXCEPT !.schedulerPaused = FALSE, !.a = "done",
    !.overwritten = q.schedulerPaused /\ q.marker = "B" /\ q.b = "ack"]
BAck ==
 /\ q.b = "ack"
 /\ q' = [q EXCEPT !.b = IF q.schedulerPaused THEN "released" ELSE "error"]
Next == AWorkerCleanup \/ BPause \/ BPrepare \/ ASchedulerCleanup \/ BAck
 \/ UNCHANGED q
Spec == Init /\ [][Next]_vars
NoSchedulerPauseOverwrite == ~q.overwritten
WorkerPausePreserved == q.marker = "B" => q.workerPaused
NoSpuriousAckError == q.b # "error"
=============================================================================
