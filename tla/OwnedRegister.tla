----------------------------- MODULE OwnedRegister -----------------------------
(***************************************************************************)
(* Safety spec for the OwnedProposer fast path composed with a full        *)
(* CASPaxos proposer on one register — the W0 ballot-space discipline      *)
(* (docs/plans/quepaxa-learnings-implementation.md).                       *)
(*                                                                         *)
(* Ballots are (epoch, seq, node): the Go implementation packs epoch and   *)
(* seq into one counter (epoch<<40 | seq) with node id as the tiebreak, so *)
(* the order is lexicographic on (e, s, n). An owner prepares ONCE at      *)
(* (e, 0, o) and then accepts at (e, s, o), s >= 1, without re-preparing — *)
(* safe only while nothing else mints ballots inside epoch e. The full     *)
(* proposer runs classic prepare/accept; the discipline under test is how  *)
(* it advances past a conflict c:                                          *)
(*                                                                         *)
(*   JumpRule = TRUE  : next epoch boundary (c.e + 1, 0, n)   [the fix]    *)
(*   JumpRule = FALSE : bump by one         (c.e, c.s + 1, n) [the bug]    *)
(*                                                                         *)
(* With JumpRule = FALSE and OwnerNode > FullNode, TLC finds the lost      *)
(* update: the full proposer commits inside the owner's epoch, and the     *)
(* owner's next seq-bumped accept — carrying a value derived from its      *)
(* stale cache — wins the node tiebreak and overwrites it.                 *)
(* OwnedRegisterBug.cfg is that negative control and MUST fail;            *)
(* OwnedRegister.cfg (JumpRule = TRUE) must pass.                          *)
(*                                                                         *)
(* CasPaxosMvcc.tla cannot see this bug class: its VoteFor requires        *)
(* SafeAt(b, v), i.e. every vote is safe by construction — exactly the     *)
(* obligation the fast path discharges by external argument instead. This  *)
(* module models that argument.                                            *)
(*                                                                         *)
(* Deliberate abstractions (documented honestly):                          *)
(* - Prepare is quorum-atomic and a failed prepare mutates nothing;        *)
(*   accepts are delivered per-acceptor with arbitrary interleaving. The   *)
(*   message-level agreement engine is proved separately (CasPaxosMvcc).   *)
(* - Values are sets of ops and every write adds one op to the value it    *)
(*   derived from. Register linearizability then implies every newly       *)
(*   committed value contains the previously committed one. Because at     *)
(*   most one value can be quorum-committed in any single state (two       *)
(*   committing quorums intersect, and the shared acceptor pins one        *)
(*   ballot and one value) and set inclusion is transitive, checking each  *)
(*   new committed value against the last one (lastC / bad below) is       *)
(*   equivalent to checking the whole chain — and costs one variable       *)
(*   instead of an ever-growing history set.                               *)
(* - Proposers over-approximate the Go code (an owner may keep writing     *)
(*   after a failed accept; the full proposer may re-propose): more        *)
(*   behaviors checked, so a pass is at least as strong.                   *)
(* - Acceptors are symmetric (quorums are cardinality-based) and the spec  *)
(*   avoids CHOOSE over acceptor sets, so SYMMETRY on Acceptor is sound.   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Acceptor,   \* acceptor set (symmetric)
          OwnerNode,  \* owner's node id (tiebreak); > FullNode is the dangerous order
          FullNode,   \* full proposer's node id
          OwnerOps,   \* ops the owner may write
          FullOps,    \* ops the full proposer may write (disjoint from OwnerOps)
          MaxEpoch,   \* epoch horizon
          MaxSeq,     \* per-epoch sequence horizon
          JumpRule    \* TRUE = the W0 epoch-jump discipline

ASSUME OwnerNode # FullNode
ASSUME OwnerOps \cap FullOps = {}

Perms == Permutations(Acceptor)

Quorum == {Q \in SUBSET Acceptor : 2 * Cardinality(Q) > Cardinality(Acceptor)}

Bal(e, s, n) == [e |-> e, s |-> s, n |-> n]
Bot == Bal(-1, -1, -1)

Less(x, y) == \/ x.e < y.e
              \/ /\ x.e = y.e
                 /\ \/ x.s < y.s
                    \/ /\ x.s = y.s
                       /\ x.n < y.n
Leq(x, y) == Less(x, y) \/ x = y

NoProp == [b |-> Bot, v |-> {}]  \* sentinel: no accept in flight

VARIABLES
  promise,  \* promise[a]: highest ballot acceptor a has promised or accepted at
  accBal,   \* accBal[a]: ballot of a's accepted value (Bot if none)
  accVal,   \* accVal[a]: a's accepted value (a set of ops)
  lastC,    \* the most recently quorum-committed value ({} initially)
  bad,      \* TRUE iff some committed value failed to contain its predecessor
  oEpoch, oSeq, oCache, oLive, oProp,  \* owner state (+ in-flight accept)
  fFloor, fBal, fRead, fPrep, fProp    \* full-proposer state (+ in-flight accept)

vars == <<promise, accBal, accVal, lastC, bad,
          oEpoch, oSeq, oCache, oLive, oProp,
          fFloor, fBal, fRead, fPrep, fProp>>

\* The value carried by the highest accepted ballot within quorum Q — what a
\* phase-1 round reads and carries forward. CHOOSE ranges over ballots and
\* values only (never over the symmetric Acceptor set).
ReadFrom(Q) ==
  LET bals == {accBal[a] : a \in Q} \ {Bot}
  IN IF bals = {}
     THEN {}
     ELSE LET mb == CHOOSE b \in bals : \A b2 \in bals : Leq(b2, b)
          IN CHOOSE v \in {accVal[a] : a \in {aa \in Q : accBal[aa] = mb}} : TRUE

\* Values quorum-committed in acceptor state (bal, val). At most one value can
\* be committed in any state (intersecting quorums), so this is {} or {v}.
CommittedIn(bal, val) ==
  {v \in {val[a] : a \in Acceptor} :
     \E Q \in Quorum :
       \E b \in {bal[a] : a \in Acceptor} :
         /\ b # Bot
         /\ \A a \in Q : bal[a] = b /\ val[a] = v}

\* Chain bookkeeping after an accept delivery: record the newly committed value
\* (if any) and flag it if it does not contain its predecessor — a lost update.
RecordCommit ==
  LET cs == CommittedIn(accBal', accVal')
  IN IF cs = {}
     THEN UNCHANGED <<lastC, bad>>
     ELSE LET c == CHOOSE v \in cs : TRUE
          IN /\ lastC' = c
             \* Parenthesized: = binds tighter than \/, and the unparenthesized
             \* form would leave bad' unassigned on exactly the violating step.
             /\ bad' = (bad \/ ~(lastC \subseteq c))

Init ==
  /\ promise = [a \in Acceptor |-> Bot]
  /\ accBal  = [a \in Acceptor |-> Bot]
  /\ accVal  = [a \in Acceptor |-> {}]
  /\ lastC = {} /\ bad = FALSE
  /\ oEpoch = 0 /\ oSeq = 0 /\ oCache = {} /\ oLive = FALSE /\ oProp = NoProp
  /\ fFloor = Bot /\ fBal = Bot /\ fRead = {} /\ fPrep = FALSE /\ fProp = NoProp

(***************************************************************************)
(* Owner actions — OwnedProposer                                           *)
(***************************************************************************)

\* TakeOwnership: one quorum-atomic phase-1 round at (e, 0, OwnerNode),
\* carrying the highest accepted value forward into the cache.
OwnerTake(e, Q) ==
  /\ e >= 1 /\ e > oEpoch
  /\ \A a \in Q : Less(promise[a], Bal(e, 0, OwnerNode))
  /\ promise' = [a \in Acceptor |->
                   IF a \in Q THEN Bal(e, 0, OwnerNode) ELSE promise[a]]
  /\ oEpoch' = e /\ oSeq' = 0 /\ oCache' = ReadFrom(Q) /\ oLive' = TRUE
  /\ oProp' = NoProp
  /\ UNCHANGED <<accBal, accVal, lastC, bad, fFloor, fBal, fRead, fPrep, fProp>>

\* Write: NO phase 1. Derive the next value from the local cache, bump seq,
\* put the accept in flight. This is the action whose safety the ballot-space
\* discipline must protect.
OwnerWrite(op) ==
  /\ oLive /\ oSeq < MaxSeq /\ oProp = NoProp
  /\ oSeq' = oSeq + 1
  /\ oCache' = oCache \cup {op}
  /\ oProp' = [b |-> Bal(oEpoch, oSeq + 1, OwnerNode), v |-> oCache \cup {op}]
  /\ UNCHANGED <<promise, accBal, accVal, lastC, bad, oEpoch, oLive,
                 fFloor, fBal, fRead, fPrep, fProp>>

\* Deliver the in-flight accept to one acceptor (acceptor.go Accept: taken iff
\* the ballot is at or above the promise).
OwnerDeliver(a) ==
  /\ oProp # NoProp
  /\ Leq(promise[a], oProp.b)
  /\ promise' = [promise EXCEPT ![a] = oProp.b]
  /\ accBal'  = [accBal  EXCEPT ![a] = oProp.b]
  /\ accVal'  = [accVal  EXCEPT ![a] = oProp.v]
  /\ RecordCommit
  /\ UNCHANGED <<oEpoch, oSeq, oCache, oLive, oProp,
                 fFloor, fBal, fRead, fPrep, fProp>>

\* The owner moves on from an in-flight write (delivered to a quorum, a subset,
\* or nobody — all interleavings covered by when deliveries happened).
OwnerFinish ==
  /\ oProp # NoProp
  /\ oProp' = NoProp
  /\ UNCHANGED <<promise, accBal, accVal, lastC, bad,
                 oEpoch, oSeq, oCache, oLive,
                 fFloor, fBal, fRead, fPrep, fProp>>

(***************************************************************************)
(* Full-proposer actions — Proposer.Propose                                 *)
(***************************************************************************)

\* The discipline under test: how the next ballot is minted past floor c.
NextBal(c) ==
  IF c = Bot THEN Bal(0, 1, FullNode)
  ELSE IF JumpRule /\ c.e >= 1 THEN Bal(c.e + 1, 0, FullNode)
  ELSE Bal(c.e, c.s + 1, FullNode)

\* Quorum-atomic phase 1 at the minted ballot; reads the carry-forward value.
FullPrepare(Q) ==
  /\ ~fPrep
  /\ LET b == NextBal(fFloor) IN
       /\ b.e <= MaxEpoch /\ b.s <= MaxSeq
       /\ \A a \in Q : Less(promise[a], b)
       /\ promise' = [a \in Acceptor |-> IF a \in Q THEN b ELSE promise[a]]
       /\ fBal' = b /\ fRead' = ReadFrom(Q) /\ fPrep' = TRUE
  /\ UNCHANGED <<accBal, accVal, lastC, bad, oEpoch, oSeq, oCache, oLive,
                 oProp, fFloor, fProp>>

\* A NACK: some acceptor's promise dominates the current attempt; adopt it as
\* the floor (Propose's floor.Max(conflict)) and restart the round.
FullConflict(a) ==
  /\ Less(fFloor, promise[a])
  /\ fFloor' = promise[a]
  /\ fPrep' = FALSE
  /\ fProp' = NoProp
  /\ UNCHANGED <<promise, accBal, accVal, lastC, bad,
                 oEpoch, oSeq, oCache, oLive, oProp, fBal, fRead>>

\* Phase 2 begins: write the read value plus one op at the prepared ballot.
FullAcceptStart(op) ==
  /\ fPrep /\ fProp = NoProp
  /\ fProp' = [b |-> fBal, v |-> fRead \cup {op}]
  /\ UNCHANGED <<promise, accBal, accVal, lastC, bad,
                 oEpoch, oSeq, oCache, oLive, oProp,
                 fFloor, fBal, fRead, fPrep>>

FullDeliver(a) ==
  /\ fProp # NoProp
  /\ Leq(promise[a], fProp.b)
  /\ promise' = [promise EXCEPT ![a] = fProp.b]
  /\ accBal'  = [accBal  EXCEPT ![a] = fProp.b]
  /\ accVal'  = [accVal  EXCEPT ![a] = fProp.v]
  /\ RecordCommit
  /\ UNCHANGED <<oEpoch, oSeq, oCache, oLive, oProp,
                 fFloor, fBal, fRead, fPrep, fProp>>

\* The round ends; the proposer's counter stays monotonic (p.counter), so the
\* floor adopts the ballot it just used.
FullFinish ==
  /\ fProp # NoProp
  /\ fProp' = NoProp
  /\ fPrep' = FALSE
  /\ fFloor' = IF Less(fFloor, fBal) THEN fBal ELSE fFloor
  /\ UNCHANGED <<promise, accBal, accVal, lastC, bad,
                 oEpoch, oSeq, oCache, oLive, oProp, fBal, fRead>>

Next ==
  \/ \E e \in 1..MaxEpoch, Q \in Quorum : OwnerTake(e, Q)
  \/ \E op \in OwnerOps : OwnerWrite(op)
  \/ \E a \in Acceptor : OwnerDeliver(a)
  \/ OwnerFinish
  \/ \E Q \in Quorum : FullPrepare(Q)
  \/ \E a \in Acceptor : FullConflict(a)
  \/ \E op \in FullOps : FullAcceptStart(op)
  \/ \E a \in Acceptor : FullDeliver(a)
  \/ FullFinish

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* Every write derives its value from the value it read, so linearizability of
\* the register means every newly committed value contains the previous one —
\* the chain property, tracked incrementally by RecordCommit. bad = TRUE is a
\* lost update: a committed value vanished from its successor.
NoLostUpdate == ~bad

\* Two acceptors accepting at the same ballot hold the same value (the S1
\* analog; ballots are unique per proposer by construction, so this guards the
\* model, not the protocol).
OneValuePerBallot ==
  \A a1, a2 \in Acceptor :
    (accBal[a1] = accBal[a2] /\ accBal[a1] # Bot) => accVal[a1] = accVal[a2]

Inv ==
  /\ NoLostUpdate
  /\ OneValuePerBallot

THEOREM Spec => []Inv
================================================================================
