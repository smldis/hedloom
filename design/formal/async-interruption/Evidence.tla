---------------------------- MODULE Evidence ----------------------------
EXTENDS Naturals, FiniteSets
CONSTANT AllowManualReuse
VARIABLE e
vars == <<e>>
Owners == {"one", "two"}
Init == e = [consumers |-> {"one"}, pending |-> {}, joined |-> FALSE,
 closing |-> FALSE, phase |-> "idle", force |-> FALSE,
 gate |-> "ready", intent |-> FALSE, body |-> FALSE,
 observation |-> "none", manifest |-> "none", terminal |-> FALSE,
 accepted |-> FALSE, reused |-> FALSE, interruptedShared |-> FALSE]
\* Pending durable bindings already reserve ownership before their write.
Reserve == /\ ~e.closing /\ ~e.joined /\ ~e.terminal
           /\ e' = [e EXCEPT !.pending = {"two"}, !.joined = TRUE]
Bind == /\ "two" \in e.pending
        /\ e' = [e EXCEPT !.pending = {}, !.consumers = @ \cup {"two"}]
Enter == /\ e.gate = "ready"
         /\ e' = [e EXCEPT !.gate = "entered", !.body = TRUE]
Request == /\ e.phase = "idle"
           /\ e' = [e EXCEPT !.phase = "withdraw"]
Escalate == /\ ~e.force
            /\ e' = [e EXCEPT !.force = TRUE,
                 !.phase = IF e.phase = "drain" THEN "withdraw" ELSE @]
Withdraw ==
 /\ e.phase = "withdraw"
 /\ IF Cardinality(e.consumers \cup e.pending) > 1 THEN
      e' = [e EXCEPT !.consumers = @ \ {"one"}, !.phase = "detached"]
    ELSE e' = [e EXCEPT !.closing = TRUE, !.phase = "gate"]
Gate ==
 /\ e.phase = "gate"
 /\ e' = [e EXCEPT !.gate = IF @ = "ready" THEN "cancelled" ELSE @,
                  !.phase = "recheck"]
Recheck ==
 /\ e.phase = "recheck"
 /\ IF Cardinality(e.consumers \cup e.pending) > 1 THEN
      e' = [e EXCEPT !.closing = FALSE, !.consumers = @ \ {"one"},
                     !.phase = "detached"]
    ELSE IF e.gate = "cancelled" THEN
      e' = [e EXCEPT !.phase = "stopped"]
    ELSE IF e.force THEN
      e' = [e EXCEPT !.intent = @ \/ e.gate = "entered",
          !.phase = IF e.gate = "entered" THEN "interrupt" ELSE "stopped"]
    ELSE e' = [e EXCEPT !.closing = FALSE, !.phase = "drain"]
\* This is an assumption boundary: PoolInterrupt separately checks whether
\* returning a cancellation observation can be trusted under its environment.
ConfirmedInterrupt ==
 /\ e.phase = "interrupt" /\ e.observation = "none"
 /\ e' = [e EXCEPT !.body = FALSE, !.observation = "cancelled",
       !.interruptedShared = Cardinality(e.consumers \cup e.pending) > 1]
NaturalFinish ==
 /\ e.body /\ e.observation = "none"
 /\ e' = [e EXCEPT !.body = FALSE, !.observation = "succeeded"]
\* Runtime interrupt.json is distinct from Exec request_cancel's journal
\* event; actual successful completion can win the pooled force race.
PublishManifest ==
 /\ e.observation # "none" /\ e.manifest = "none"
 /\ e' = [e EXCEPT !.manifest = e.observation]
PublishTerminal ==
 /\ e.manifest # "none" /\ ~e.terminal
 /\ e' = [e EXCEPT !.terminal = TRUE, !.gate = "finished"]
Accept ==
 /\ AllowManualReuse /\ e.terminal /\ e.manifest # "none" /\ ~e.accepted
 /\ e' = [e EXCEPT !.accepted = TRUE]
Reuse ==
 /\ e.manifest # "none" /\ (e.manifest = "succeeded" \/ e.accepted)
 /\ ~e.reused /\ e' = [e EXCEPT !.reused = TRUE]
Next == Reserve \/ Bind \/ Enter \/ Request \/ Escalate \/ Withdraw \/ Gate
 \/ Recheck \/ ConfirmedInterrupt \/ NaturalFinish \/ PublishManifest
 \/ PublishTerminal \/ Accept \/ Reuse \/ UNCHANGED e
Spec == Init /\ [][Next]_vars
SharedProtected == ~e.interruptedShared
DurableIntent == e.phase = "interrupt" => e.intent
TerminalHasManifest == e.terminal => e.manifest # "none"
NoIncompleteReuse == e.reused => e.manifest # "none" /\ ~e.body
NoCancelledReuse == e.reused => e.manifest # "cancelled"
NoAutomaticCancelledReuse == e.reused /\ ~e.accepted => e.manifest # "cancelled"
=============================================================================
