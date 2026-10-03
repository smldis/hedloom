-------------------------- MODULE LossEvidence --------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Legacy, InitiallyAssigned
\* Bound: one scheduler key, two TaskState identities, two worker identities,
\* at most two losses and one certification callback. None is a typed sentinel.
VARIABLE e
vars == <<e>>
W == {1,2}                 \* distinct WorkerState objects/addresses
T == {1,2}                 \* successive TaskState objects for one key
None == [worker |-> 0, identity |-> 0, token |-> 0]
Proof(w,t) == [worker |-> w, identity |-> w, token |-> w, task |-> t]
Pending(w) == [worker |-> w, identity |-> w, token |-> w]
Copies(t) == {p \in e.alive : p[1] = t}

Init == \E running \in BOOLEAN :
 e = [task |-> 1, loc |-> IF InitiallyAssigned THEN 1 ELSE 0,
      alive |-> IF InitiallyAssigned /\ running THEN {<<1,1>>} ELSE {},
      interest |-> TRUE, unknown |-> FALSE, everUnknown |-> FALSE,
      pending |-> None, lost |-> {}, begun |-> {}, captured |-> [w \in W |-> 0],
      killed |-> {}, ack |-> {}, result |-> "waiting", badCert |-> FALSE,
      checked |-> FALSE, extra |-> FALSE, staleIgnored |-> FALSE, certified |-> FALSE]
\* _begin_owned_restart snapshots task identity while the owned worker fence
\* is held. Fence acquisition/consumer checks are assumptions of this model.
Begin(w) ==
 /\ e.result = "waiting" /\ e.loc = w /\ w \notin e.begun /\ w \notin e.lost
 /\ e' = [e EXCEPT !.begun = @ \cup {w}, !.captured[w] = e.task]
\* OwnedPoolScheduler.remove_worker calls assignment_loss BEFORE super clears
\* processing_on, independently of expected/safe flags or suspicious counters.
Lose(w) ==
 /\ e.result = "waiting" /\ e.loc = w /\ w \notin e.lost
 /\ LET uncertain == w \notin e.begun \/ e.pending # None IN
    e' = [e EXCEPT !.loc = 0, !.lost = @ \cup {w},
      !.unknown = @ \/ uncertain, !.everUnknown = @ \/ uncertain,
      !.extra = @ \/ e.pending # None,
      !.pending = IF uncertain THEN @ ELSE Pending(w)]
\* Removal does NOT kill the body. Restart death and successful reply are
\* separate events; the reply may never arrive even after physical death.
PhysicalRestart(w) ==
 /\ w \in e.begun /\ w \in e.lost /\ w \notin e.killed
 /\ e' = [e EXCEPT !.killed = @ \cup {w},
                   !.alive = {p \in @ : p[2] # w}]
Acknowledge(w) ==
 /\ w \in e.killed /\ w \notin e.ack
 /\ e' = [e EXCEPT !.ack = @ \cup {w}]
\* Proof candidates include a delayed genuine snapshot and mismatched token,
\* address, object identity or TaskState. Only the genuine acknowledged proof
\* is emitted by the normal caller; mismatches exercise the helper contract.
Candidates(w) == {Proof(w,e.captured[w]),
 [Proof(w,e.captured[w]) EXCEPT !.worker = 3],
 [Proof(w,e.captured[w]) EXCEPT !.identity = 3],
 [Proof(w,e.captured[w]) EXCEPT !.token = 3]}
Certify(w,p) ==
 /\ w \in e.ack /\ p \in Candidates(w) /\ ~e.checked
 /\ LET matches == p.task = e.task /\ e.pending =
          [worker |-> p.worker, identity |-> p.identity, token |-> p.token]
    IN e' = [e EXCEPT !.checked = TRUE,
      !.pending = IF matches THEN None ELSE @,
      !.certified = @ \/ matches,
      !.staleIgnored = @ \/ (p.task # e.task /\ e.pending # None),
      !.badCert = @ \/ (matches /\
        (p # Proof(w,e.task) \/ w \notin e.killed))]
\* Deliberate overapproximation of the loss helper: allow reassignment even
\* while pending. Latest valid_workers/ADMISSION narrows this in runtime;
\* this model does not verify those newer admission mechanisms.
Assign(w) ==
 /\ e.result = "waiting" /\ e.interest /\ e.loc = 0 /\ w \notin e.lost
 /\ e' = [e EXCEPT !.loc = w]
Start ==
 /\ e.result = "waiting" /\ e.interest /\ e.loc # 0
 /\ <<e.task,e.loc>> \notin e.alive
 /\ e' = [e EXCEPT !.alive = @ \cup {<<e.task,e.loc>>}]
\* Recreating the scheduler key gives a fresh TaskState, not shared metadata.
\* Old bodies/proofs retain their old task identity. Cross-identity lifetime
\* safety is deliberately NOT asserted; the key is not an attempt identity.
ReplaceTask ==
 /\ e.result = "waiting" /\ e.task = 1
 /\ e' = [e EXCEPT !.task = 2, !.unknown = FALSE, !.everUnknown = FALSE,
                   !.pending = None, !.extra = FALSE, !.certified = FALSE]
\* _assignment_evidence runs at both locate and prepare; absence alone does
\* not authorize this branch. We model an existing but unassigned TaskState.
Decide ==
 /\ e.result = "waiting"
 /\ IF ~Legacy /\ e.unknown THEN e' = [e EXCEPT !.result = "error"]
    ELSE IF e.loc = 0 /\ (Legacy \/ e.pending = None) THEN
      e' = [e EXCEPT !.result = "cancelled", !.interest = FALSE]
    ELSE FALSE
Timeout == /\ e.result = "waiting"
           /\ e' = [e EXCEPT !.result = "error"]
Next == Decide \/ Timeout \/ ReplaceTask \/ Start
 \/ (\E w \in W : Begin(w) \/ Lose(w) \/ PhysicalRestart(w)
       \/ Acknowledge(w) \/ Assign(w) \/ (\E p \in Candidates(w): Certify(w,p)))
 \/ (e.result # "waiting" /\ UNCHANGED e)
Spec == Init /\ [][Next]_vars /\ WF_vars(Timeout)
NoFalseClaim == e.result = "cancelled" => Copies(e.task) = {}
NoUnsafeEvidenceClaim == e.result = "cancelled" => ~e.unknown /\ e.pending = None
StickyUnknown == e.everUnknown => e.unknown
ExactCertificate == ~e.badCert
ErrorRetainsInterest == e.result = "error" => e.interest
Settles == <> (e.result # "waiting")
\* Reachability queries: violations below are desired witnesses, not defects.
NoNeverAssignedCancellation == ~(e.result = "cancelled" /\ e.lost = {})
NoCertifiedCancellation == ~(e.result = "cancelled" /\ e.certified)
NoExtraLossSurvival == ~(e.extra /\ e.unknown /\ e.pending = None /\ e.ack # {})
NoStaleIdentityRejection == ~e.staleIgnored
NoPendingTimeout == ~(e.result = "error" /\ e.pending # None /\ e.ack = {})
=============================================================================
