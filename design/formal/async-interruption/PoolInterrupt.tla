-------------------------- MODULE PoolInterrupt --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS LegacyMemory, LegacyRPC, Faults, Shared, LateRegistration, Partition
VARIABLE s
vars == <<s>>
C == {"A", "B", "C"}
Targets == {"A", "B"}
Owners == {"one", "two"}
Addr == 1..4  \* 1,2 initially; 3,4 are fresh address generations.
None == 0
Owner(c) == IF c = "B" THEN "two" ELSE "one"
Terminal == {"cancelled", "completed", "error"}
Executing(a) == {c \in C : s.body[c] = a}
Exclusive(c) == s.interest[c] = {Owner(c)}

Init == s = [
 live |-> {1,2}, used |-> {1,2},
 loc |-> [c \in C |-> IF c = "C" THEN 2 ELSE
                  IF c = "B" /\ LateRegistration THEN 0 ELSE 1],
 registered |-> IF LateRegistration THEN {"A", "C"} ELSE C,
 interest |-> [c \in C |-> IF c = "B" /\ Shared THEN Owners ELSE {Owner(c)}],
 body |-> [c \in C |-> IF c = "A" THEN 1 ELSE IF c = "C" THEN 2 ELSE 0],
 local |-> [a \in Addr |-> IF a = 1 THEN
                  (IF LateRegistration THEN {"A"} ELSE {"A", "B"})
                  ELSE IF a = 2 THEN {"C"} ELSE {}],
 pause |-> [a \in Addr |-> FALSE], spause |-> [a \in Addr |-> FALSE],
 marker |-> [a \in Addr |-> ""], mem |-> [a \in Addr |-> TRUE],
 pc |-> [c \in Targets |-> "intent"], intent |-> {},
 address |-> [c \in Targets |-> 0], entered |-> [c \in Targets |-> FALSE],
 snapshot |-> [c \in Targets |-> {}], withdrawn |-> {}, free |-> {},
 finished |-> {}, claimed |-> {}, restarted |-> {},
 collateral |-> {}, replay |-> {}, started |-> {"A", "C"},
 staleError |-> FALSE, postWithdrawStart |-> {}, badReplay |-> {}]

Persist(c) == /\ s.pc[c] = "intent"
              /\ s' = [s EXCEPT !.intent = @ \cup {c}, !.pc[c] = "locate"]
Register == /\ "B" \notin s.registered /\ "B" \notin s.withdrawn
            /\ s' = [s EXCEPT !.registered = @ \cup {"B"}]
Locate(c) ==
 /\ s.pc[c] = "locate" /\ c \in s.registered
 /\ s' = [s EXCEPT !.address[c] = s.loc[c], !.snapshot[c] = {},
                  !.entered[c] = FALSE,
                  !.pc[c] = IF c \in s.finished THEN "completed" ELSE
                      IF ~Exclusive(c) THEN "error" ELSE
                      IF s.loc[c] = 0 THEN "prepare" ELSE "rpc"]
\* An absent scheduler key deliberately has no cancellation transition.
Pause(c) == LET a == s.address[c] IN
 /\ s.pc[c] = "rpc" /\ a \in s.live
 /\ c \in s.local[a] /\ c \notin s.finished /\ s.marker[a] = ""
 /\ ~s.pause[a]
 /\ s' = [s EXCEPT !.pause[a] = TRUE, !.marker[a] = c,
                  !.mem[a] = IF LegacyMemory THEN @ ELSE FALSE,
                  !.entered[c] = c \in Executing(a),
                  !.snapshot[c] = Executing(a), !.pc[c] = "prepare"]
BusyOrMissing(c) == LET a == s.address[c] IN
 /\ s.pc[c] = "rpc" /\ a \in s.live
 /\ (s.marker[a] # "" \/ c \notin s.local[a] \/ c \in s.finished)
 /\ s' = [s EXCEPT !.pc[c] = "locate"]
\* Failure arrival and the subsequent authoritative scheduler observation are
\* separate steps. This matters if another restart happens between them.
RPCClosed(c) ==
 /\ s.pc[c] = "rpc" /\ (s.address[c] \notin s.live \/ Faults)
 /\ s' = [s EXCEPT !.pc[c] = IF LegacyRPC THEN "error" ELSE "recheck",
          !.staleError = @ \/ (LegacyRPC /\ s.address[c] \notin s.live)]
Recheck(c) ==
 /\ s.pc[c] = "recheck"
 /\ s' = [s EXCEPT !.pc[c] =
       IF s.address[c] \in s.live THEN "error"
       ELSE IF c \in s.registered /\ c \notin s.finished /\ ~Exclusive(c)
            THEN "error" ELSE "locate", !.address[c] =
       IF s.address[c] \in s.live THEN @ ELSE 0]
\* Lost response after the pause side effect; cleanup retains its address.
LostReply(c) == /\ Faults /\ s.pc[c] = "prepare" /\ s.address[c] # 0
                /\ s' = [s EXCEPT !.pc[c] = "recheck"]
Prepare(c) == LET a == s.loc[c] IN
 /\ s.pc[c] = "prepare"
 /\ IF c \notin s.registered THEN
       s' = [s EXCEPT !.pc[c] = "error"]
    ELSE IF c \in s.finished THEN s' = [s EXCEPT !.pc[c] = "completed"]
    ELSE IF ~Exclusive(c) THEN s' = [s EXCEPT !.pc[c] = "error"]
    ELSE IF a # 0 /\ s.address[c] = 0 THEN
       s' = [s EXCEPT !.pc[c] = "locate"]
    ELSE IF a # 0 /\ a # s.address[c] THEN
       s' = [s EXCEPT !.pc[c] = "error"]
    ELSE IF a # 0 /\ s.entered[c] /\ s.snapshot[c] # {c} THEN
       s' = [s EXCEPT !.pc[c] = "error"]
    ELSE s' = [s EXCEPT !.interest[c] = {}, !.registered = @ \ {c},
                !.withdrawn = @ \cup {c}, !.free = @ \cup {c},
                !.address[c] = a, !.loc[c] = 0,
                !.spause = IF a = 0 THEN @ ELSE [@ EXCEPT ![a] = TRUE],
                !.pc[c] = "ack"]
Ack(c) == LET a == s.address[c] IN
 /\ s.pc[c] = "ack"
 /\ s' = [s EXCEPT !.pc[c] = IF c \in s.registered THEN "ack" ELSE
       IF a # 0 /\ (a \notin s.live \/ ~s.spause[a]) THEN "error" ELSE
       IF a # 0 /\ s.entered[c] THEN "restart" ELSE "release"]
FreeKey(c) ==
 /\ c \in s.free /\ s.body[c] = 0
 /\ s' = [s EXCEPT !.free = @ \ {c},
                   !.local = [a \in Addr |-> s.local[a] \ {c}]]
ClaimQueued(c) == LET a == s.address[c] IN
 /\ s.pc[c] = "release"
 /\ IF a = 0 THEN TRUE ELSE c \notin s.local[a]
 /\ s' = [s EXCEPT !.pc[c] = "cancelled", !.claimed = @ \cup {c}]
\* Restart has NO guard against collateral bodies: snapshot/fence must suffice.
Restart(c) == LET a == s.address[c] IN
 /\ s.pc[c] = "restart" /\ a \in s.live /\ a \in {1,2}
 /\ s' = [s EXCEPT !.live = @ \ {a}, !.local[a] = {},
    !.body = [d \in C |-> IF s.body[d] = a THEN 0 ELSE s.body[d]],
    !.loc = [d \in C |-> IF s.loc[d] = a THEN 0 ELSE s.loc[d]],
    !.collateral = @ \cup (Executing(a) \ {c}),
    !.restarted = @ \cup {c}, !.pc[c] = "confirm"]
Confirm(c) ==
 /\ s.pc[c] = "confirm" /\ s.address[c] + 2 \in s.live
 /\ s' = [s EXCEPT !.pc[c] = "cancelled", !.claimed = @ \cup {c}]
Replacement(a) ==
 /\ a \in {1,2} /\ a \notin s.live /\ a + 2 \notin s.used
 /\ s' = [s EXCEPT !.live = @ \cup {a+2}, !.used = @ \cup {a+2}]
\* Fresh addresses start with Init's clean marker/memory policy. No reuse of
\* addresses; at most one replacement generation per physical worker.
Cleanup(c) == LET a == s.address[c] IN
 /\ s.pc[c] \in Terminal /\ a \in s.live /\ s.marker[a] = c
 /\ c \notin s.restarted
 /\ s' = [s EXCEPT !.marker[a] = "", !.pause[a] = FALSE,
                   !.mem[a] = TRUE, !.spause[a] = FALSE]
WrongCleanup(c,a) ==
 /\ a \in s.live /\ s.marker[a] # c /\ UNCHANGED s
MemoryResume(a) == /\ a \in s.live /\ s.pause[a] /\ s.mem[a]
                   /\ s' = [s EXCEPT !.pause[a] = FALSE]
Assign(c,a) ==
 /\ c \in s.registered /\ c \notin s.finished /\ s.loc[c] = 0
 /\ a \in s.live /\ ~s.spause[a] /\ s.interest[c] # {}
 /\ s' = [s EXCEPT !.loc[c] = a, !.local[a] = @ \cup {c}]
Start(c,a) ==
 /\ a \in s.live /\ c \in s.local[a] /\ s.body[c] = 0
 /\ c \notin s.finished /\ ~s.pause[a] /\ Executing(a) = {}
 /\ s' = [s EXCEPT !.body[c] = a, !.started = @ \cup {c},
                   !.replay = IF c \in s.started THEN @ \cup {c} ELSE @,
                   !.postWithdrawStart = IF c \in s.withdrawn THEN @ \cup {c} ELSE @,
                   !.badReplay = IF c \in s.restarted THEN @ \cup {c} ELSE @]
Finish(c) == /\ s.body[c] # 0
             /\ s' = [s EXCEPT !.body[c] = 0, !.finished = @ \cup {c}]
UnexpectedLoss(a) ==
 /\ Faults /\ a \in s.live /\ a \in {1,2}
 /\ s' = [s EXCEPT !.live = @ \ {a}, !.local[a] = {},
    !.body = [c \in C |-> IF s.body[c] = a /\ ~Partition THEN 0 ELSE s.body[c]],
    !.loc = [c \in C |-> IF s.loc[c] = a THEN 0 ELSE s.loc[c]]]
Deadline(c) == /\ s.pc[c] \notin Terminal
               /\ s' = [s EXCEPT !.pc[c] = "error"]
PerCommand(c) == Persist(c) \/ Locate(c) \/ Pause(c) \/ BusyOrMissing(c)
 \/ RPCClosed(c) \/ Recheck(c) \/ LostReply(c) \/ Prepare(c) \/ Ack(c)
 \/ ClaimQueued(c) \/ Restart(c) \/ Confirm(c) \/ Cleanup(c) \/ Deadline(c)
Next == (\E c \in Targets : PerCommand(c)) \/ Register
 \/ (\E c \in C : FreeKey(c) \/ Finish(c))
 \/ (\E a \in Addr : MemoryResume(a) \/ Replacement(a) \/ UnexpectedLoss(a)
          \/ (\E c \in C : Assign(c,a) \/ Start(c,a)))
 \* Quiescence is legal, not a promise of successful cancellation.
 \/ ((\A c \in Targets : s.pc[c] \in Terminal) /\ UNCHANGED s)
Spec == Init /\ [][Next]_vars
TimedSpec == Spec /\ (\A c \in Targets : WF_vars(Deadline(c)))
Settles == <> (\A c \in Targets : s.pc[c] \in Terminal)
SharedSurvives == Shared => "B" \notin s.restarted /\ "B" \notin s.collateral
NoCollateral == s.collateral = {}
NoFalseTermination == \A c \in s.claimed : s.body[c] = 0
WithdrawBeforeRestart == s.restarted \subseteq s.withdrawn
DurableBeforeWithdraw == s.withdrawn \subseteq s.intent
NoIntentionalReplay == s.badReplay = {}
Reservation == \A a \in Addr : Cardinality(Executing(a)) <= 1
Fence == \A a \in s.live : s.marker[a] # "" => s.pause[a]
NoStaleRPCFailure == ~s.staleError
NoPostWithdrawalStart == s.postWithdrawStart = {}
NoReplay == s.replay = {}
TypeOK == /\ s.live \subseteq Addr /\ s.registered \subseteq C
 /\ s.body \in [C -> Addr \cup {0}] /\ s.loc \in [C -> Addr \cup {0}]
 /\ s.interest \in [C -> SUBSET Owners]
 /\ s.pc \in [Targets -> {"intent", "locate", "rpc", "recheck", "prepare",
              "ack", "release", "restart", "confirm"} \cup Terminal]
=============================================================================
