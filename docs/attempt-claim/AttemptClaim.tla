------------------------------ MODULE AttemptClaim ------------------------------
(***************************************************************************)
(* A TLA+ model of Hedloom's attempt protocol: `AttemptJournal.claim`,      *)
(* `launch_or_attach`, `reconcile` and `publish_terminal` in `hedloom_exec`, *)
(* as run concurrently by several callers against one attempt directory.    *)
(*                                                                         *)
(* It models one identity -- one sequence of one invocation -- because that *)
(* is where every synchronisation decision lands. Different inputs derive a *)
(* different identity and share nothing, so N identities are N independent  *)
(* copies of this model.                                                   *)
(*                                                                         *)
(* Abstracted away: the bytes of the journal, the filesystem, weak memory.  *)
(* Every action below is atomic and TLC explores all interleavings, which   *)
(* is enough for the algorithmic claims, because those rest on *which*      *)
(* durable fact is consulted and *in what order* the two durable writes     *)
(* happen -- not on the width of any individual write.                     *)
(*                                                                         *)
(* Kept faithful on purpose:                                               *)
(*                                                                         *)
(*   * `Fold` is `journal.fold()` transcribed: a left fold over the event   *)
(*     log, later events overriding earlier ones.                          *)
(*   * The claim is taken before the record is read and released when       *)
(*     `launch_or_attach` returns -- so `reconcile` and `publish_terminal`  *)
(*     run *unlocked*, exactly as they do today.                           *)
(*   * `bsub -I` blocks until the job is over, so submission is two steps   *)
(*     (start, return) with the job live in between.                       *)
(*   * A crash kills that caller's farm job. That is owner-bound lifetime,  *)
(*     and it is an assumption about the transport, not a theorem.         *)
(*                                                                         *)
(* Five constants make load-bearing assumptions switchable, so that denying *)
(* one and re-running TLC shows what it was holding up. See the MC*.cfg     *)
(* files and `docs/attempt-claim-protocol.md`.                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Callers,             \* concurrent controllers/threads against one attempt
    Outcomes,            \* what a poll may report, e.g. {"succeeded", "unreconciled"}
    MaxCrashes,          \* bound on modelled crashes, to keep the space finite
    LockHonoured,        \* TRUE: flock excludes. FALSE: NFS local_lock=all
    DiscoveryIsAccurate, \* TRUE: `discover` sees a live job iff one exists
    OwnerBoundLifetime,  \* TRUE: a dead caller's farm job dies with it
    PublishUnderClaim,   \* FALSE: as shipped. TRUE: hold the claim through publication
    PublishOrder,        \* "manifest-first" (shipped) | "record-first" (mutation)
    RefusesWhenBlind     \* TRUE: as shipped -- a blind transport raises rather than guessing

NoOne     == "no-one"
NoOutcome == "no-outcome"

ASSUME NoOne \notin Callers
ASSUME NoOutcome \notin Outcomes
ASSUME PublishOrder \in {"manifest-first", "record-first"}

VARIABLES
    pc,         \* pc[c]: where caller c is in the protocol
    holder,     \* who holds claim.lock: NoOne or a caller
    log,        \* the durable journal: a sequence of event names
    published,  \* manifest.json: NoOutcome, or the outcome a rename made visible
    recorded,   \* the outcome named by the last "terminal" event in the journal
    seen,       \* seen[c]: what caller c's poll observed
    live,       \* live[c]: caller c's farm job is running right now
    crashes

vars == <<pc, holder, log, published, recorded, seen, live, crashes>>

Settled(c) == pc[c] \in {"done", "refused", "dead"}
AllSettled == \A c \in Callers : Settled(c)

----------------------------------------------------------------------------
(* `journal.fold()`, transcribed. The phases are the ones AttemptState names: *)
(* unsubmitted, intended (the crash window), submitted, terminal.             *)

StepPhase(phase, event) ==
    CASE event = "submit_intent"   -> "intended"
      [] event = "submit_receipt"  -> "submitted"
      [] event = "submit_lost"     -> "unsubmitted"
      [] event = "terminal"        -> "terminal"
      [] OTHER                     -> phase

RECURSIVE Fold(_, _)
Fold(phase, events) ==
    IF events = <<>> THEN phase
    ELSE Fold(StepPhase(phase, Head(events)), Tail(events))

Phase == Fold("unsubmitted", log)

(* A live job with this identity exists on the substrate. `discover` is       *)
(* declared authoritative by LSFInteractiveTransport on the strength of       *)
(* lookup by job name; denying this constant is what a site whose `bjobs`     *)
(* cannot answer for a just-submitted job actually looks like.                *)
DiscoveryFinds == DiscoveryIsAccurate /\ (\E c \in Callers : live[c])

ReleasedBy(c) == IF holder = c THEN NoOne ELSE holder

(* Where a caller goes once it holds a receipt or has attached: straight to   *)
(* the poll if publication is meant to stay under the claim, otherwise out    *)
(* of the lock first, which is what `launch_or_attach` returning does today.  *)
AfterLaunch == IF PublishUnderClaim THEN "poll" ELSE "release"

FirstPublicationStep == IF PublishOrder = "manifest-first" THEN "publish" ELSE "record"

----------------------------------------------------------------------------
TypeOK ==
    /\ pc \in [Callers -> {"select", "claim", "decide", "intent", "submit",
                           "running", "receipt", "release", "poll", "publish",
                           "record", "done", "refused", "dead"}]
    /\ holder \in Callers \cup {NoOne}
    /\ published \in Outcomes \cup {NoOutcome}
    /\ recorded \in Outcomes \cup {NoOutcome}
    /\ seen \in [Callers -> Outcomes \cup {NoOutcome}]
    /\ live \in [Callers -> BOOLEAN]
    /\ crashes \in 0..MaxCrashes

Init ==
    /\ pc        = [c \in Callers |-> "select"]
    /\ holder    = NoOne
    /\ log       = <<>>
    /\ published = NoOutcome
    /\ recorded  = NoOutcome
    /\ seen      = [c \in Callers |-> NoOutcome]
    /\ live      = [c \in Callers |-> FALSE]
    /\ crashes   = 0

----------------------------------------------------------------------------
(* `_select_sequence`, which reads published manifests *outside* any claim.   *)
(* One sequence is modelled, so a visible manifest means this run reuses it.  *)
Select(c) ==
    /\ pc[c] = "select"
    /\ pc' = [pc EXCEPT ![c] = IF published # NoOutcome THEN "done" ELSE "claim"]
    /\ UNCHANGED <<holder, log, published, recorded, seen, live, crashes>>

(* `journal.claim()`. Non-blocking: a second holder is reported as            *)
(* ConcurrentClaim rather than waited for, because a second submission of one *)
(* attempt is the defect. Denying LockHonoured models a study root on an NFS  *)
(* mount that answers flock locally -- the lock is taken and means nothing.   *)
Claim(c) ==
    /\ pc[c] = "claim"
    /\ IF holder = NoOne \/ ~LockHonoured
         THEN /\ holder' = c
              /\ pc' = [pc EXCEPT ![c] = "decide"]
         ELSE /\ pc' = [pc EXCEPT ![c] = "refused"]
              /\ UNCHANGED holder
    /\ UNCHANGED <<log, published, recorded, seen, live, crashes>>

(* `_launch_or_attach_locked`: read the manifest, fold the record, and resolve *)
(* to completed / attached / claimed -- or refuse to guess.                    *)
Decide(c) ==
    /\ pc[c] = "decide"
    /\ \/ \* completed: a manifest is visible, so the payload does not rerun
          /\ published # NoOutcome
          /\ pc' = [pc EXCEPT ![c] = "done"]
          /\ holder' = ReleasedBy(c)
          /\ UNCHANGED log
       \/ \* ReconciliationError: terminal claimed, no evidence behind it
          /\ published = NoOutcome
          /\ Phase = "terminal"
          /\ pc' = [pc EXCEPT ![c] = "refused"]
          /\ holder' = ReleasedBy(c)
          /\ UNCHANGED log
       \/ \* attached: the substrate already holds this attempt
          /\ published = NoOutcome
          /\ Phase = "submitted"
          /\ pc' = [pc EXCEPT ![c] = AfterLaunch]
          /\ UNCHANGED <<holder, log>>
       \/ \* the crash window: intent is durable, acceptance is unknown
          /\ published = NoOutcome
          /\ Phase = "intended"
          /\ IF DiscoveryFinds
               THEN /\ log' = Append(log, "submit_receipt")
                    /\ pc' = [pc EXCEPT ![c] = AfterLaunch]
                    /\ UNCHANGED holder
               \* `UnrecoverableAttempt`. A transport that cannot confirm or
               \* deny acceptance is not allowed to be read as denying it, so
               \* the caller refuses rather than guessing. This is the branch
               \* the protocol document called unreachable "because nothing
               \* detaches" -- pooled placement is the thing that detaches.
               ELSE IF RefusesWhenBlind /\ ~DiscoveryIsAccurate
                 THEN /\ pc' = [pc EXCEPT ![c] = "refused"]
                      /\ holder' = ReleasedBy(c)
                      /\ UNCHANGED log
                 ELSE /\ log' = Append(log, "submit_lost")
                      /\ pc' = [pc EXCEPT ![c] = "intent"]
                      /\ UNCHANGED holder
       \/ \* nothing was ever accepted: this call submits it once
          /\ published = NoOutcome
          /\ Phase = "unsubmitted"
          /\ log' = Append(log, "created")
          /\ pc' = [pc EXCEPT ![c] = "intent"]
          /\ UNCHANGED holder
    /\ UNCHANGED <<published, recorded, seen, live, crashes>>

(* Intent is durable before the substrate is touched. Everything downstream   *)
(* depends on this ordering.                                                  *)
Intent(c) ==
    /\ pc[c] = "intent"
    /\ log' = Append(log, "submit_intent")
    /\ pc' = [pc EXCEPT ![c] = "submit"]
    /\ UNCHANGED <<holder, published, recorded, seen, live, crashes>>

(* `bsub -I` starts the job and blocks. The job is live from here until the   *)
(* call returns -- or until this caller dies, which under owner-bound         *)
(* lifetime takes the job with it.                                           *)
SubmitStart(c) ==
    /\ pc[c] = "submit"
    /\ live' = [live EXCEPT ![c] = TRUE]
    /\ pc' = [pc EXCEPT ![c] = "running"]
    /\ UNCHANGED <<holder, log, published, recorded, seen, crashes>>

SubmitReturn(c) ==
    /\ pc[c] = "running"
    /\ live' = [live EXCEPT ![c] = FALSE]
    /\ pc' = [pc EXCEPT ![c] = "receipt"]
    /\ UNCHANGED <<holder, log, published, recorded, seen, crashes>>

Receipt(c) ==
    /\ pc[c] = "receipt"
    /\ log' = Append(log, "submit_receipt")
    /\ pc' = [pc EXCEPT ![c] = AfterLaunch]
    /\ UNCHANGED <<holder, published, recorded, seen, live, crashes>>

(* `launch_or_attach` returns and the claim goes with it. *)
Release(c) ==
    /\ pc[c] = "release"
    /\ holder' = ReleasedBy(c)
    /\ pc' = [pc EXCEPT ![c] = "poll"]
    /\ UNCHANGED <<log, published, recorded, seen, live, crashes>>

(* `reconcile`. Its first act is to re-read the manifest and return if one is *)
(* already visible -- a guard that is only as good as the window it is read   *)
(* in. Then it polls the substrate and stages an outcome to publish.          *)
ReconcileNoop(c) ==
    /\ pc[c] = "poll"
    /\ published # NoOutcome
    /\ pc' = [pc EXCEPT ![c] = "done"]
    /\ holder' = ReleasedBy(c)
    /\ UNCHANGED <<log, published, recorded, seen, live, crashes>>

Poll(c) ==
    /\ pc[c] = "poll"
    /\ published = NoOutcome
    /\ \E o \in Outcomes :
        /\ seen' = [seen EXCEPT ![c] = o]
        /\ pc' = [pc EXCEPT ![c] = FirstPublicationStep]
    /\ UNCHANGED <<holder, log, published, recorded, live, crashes>>

(* `publish_terminal`, whose two durable writes are the whole recovery        *)
(* argument: the manifest is made atomically visible by rename, and only then *)
(* is the terminal outcome recorded. Reversing them is a mutation, not a      *)
(* refactor.                                                                  *)
Publish(c) ==
    /\ pc[c] = "publish"
    /\ published' = seen[c]
    /\ IF PublishOrder = "manifest-first"
         THEN /\ pc' = [pc EXCEPT ![c] = "record"]
              /\ UNCHANGED holder
         ELSE /\ pc' = [pc EXCEPT ![c] = "done"]
              /\ holder' = ReleasedBy(c)
    /\ UNCHANGED <<log, recorded, seen, live, crashes>>

Record(c) ==
    /\ pc[c] = "record"
    /\ log' = Append(log, "terminal")
    /\ recorded' = seen[c]
    /\ IF PublishOrder = "manifest-first"
         THEN /\ pc' = [pc EXCEPT ![c] = "done"]
              /\ holder' = ReleasedBy(c)
         ELSE /\ pc' = [pc EXCEPT ![c] = "publish"]
              /\ UNCHANGED holder
    /\ UNCHANGED <<published, seen, live, crashes>>

(* The caller dies. Durable state survives; the advisory lock does not,       *)
(* because the kernel drops it when the file descriptor closes; and the farm  *)
(* job does not either -- under owner-bound lifetime its `bsub -I` client was *)
(* this process. Denying OwnerBoundLifetime leaves the job running with       *)
(* nobody attached to it, which is what any detached or pooled substrate      *)
(* would do and what the `attached` disposition was written for.              *)
Crash(c) ==
    /\ ~Settled(c)
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ pc' = [pc EXCEPT ![c] = "dead"]
    /\ holder' = ReleasedBy(c)
    /\ live' = IF OwnerBoundLifetime THEN [live EXCEPT ![c] = FALSE] ELSE live
    /\ UNCHANGED <<log, published, recorded, seen>>

(* An orphaned job -- one that outlived the caller that submitted it -- stops *)
(* on its own, at some later point. Only reachable when lifetime is not       *)
(* owner-bound.                                                               *)
(*                                                                            *)
(* This is also the delayed reaping a pooled worker does, and deliberately the *)
(* same action: a pooled invocation's command is killed when its worker hits   *)
(* `death_timeout` after the scheduler its caller owned went away. Finishing   *)
(* and being reaped differ in why `live` clears, not in when it may clear --   *)
(* both are "after an unbounded but finite delay, during which other callers   *)
(* may do anything" -- and no invariant here distinguishes work that completed *)
(* from work that was killed. Modelling them as two actions would double the   *)
(* state space to say the same thing twice.                                    *)
OrphanFinishes(c) ==
    /\ pc[c] = "dead"
    /\ live[c]
    /\ live' = [live EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<pc, holder, log, published, recorded, seen, crashes>>

Next ==
    \/ \E c \in Callers :
        \/ Select(c) \/ Claim(c) \/ Decide(c) \/ Intent(c)
        \/ SubmitStart(c) \/ SubmitReturn(c) \/ Receipt(c) \/ Release(c)
        \/ ReconcileNoop(c) \/ Poll(c) \/ Publish(c) \/ Record(c)
        \/ Crash(c) \/ OrphanFinishes(c)
    \* Every caller has finished, refused, or died. Stuttering here keeps TLC's
    \* deadlock check meaningful: it then fires only on a state where somebody
    \* is still trying to make progress and cannot.
    \/ (AllSettled /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----------------------------------------------------------------------------
(* Properties. *)

(* One identity, at most one farm job at a time. This is what the claim exists *)
(* to buy, and the only thing that costs real money when it is lost.           *)
AtMostOneLive ==
    Cardinality({c \in Callers : live[c]}) <= 1

(* A job that exists is a job the durable record can name. An accepted         *)
(* submission whose receipt is lost still has a trace to look for.             *)
LiveJobHasDurableTrace ==
    (\E c \in Callers : live[c]) => (Phase # "unsubmitted")

(* A journal that claims a terminal outcome always has readable evidence.      *)
TerminalHasEvidence ==
    recorded # NoOutcome => published # NoOutcome

(* Stronger, and the one the reuse decision actually needs: the outcome the    *)
(* record names is the outcome the visible manifest carries. `execute` returns *)
(* the phase from the journal and the artifacts from the manifest, so the two  *)
(* disagreeing is a result reported under the wrong verdict.                   *)
RecordMatchesEvidence ==
    recorded # NoOutcome => published = recorded

=============================================================================
