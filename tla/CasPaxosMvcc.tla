------------------------------ MODULE CasPaxosMvcc ------------------------------
(***************************************************************************)
(* Safety specification for the consensus engine underneath each cask key. *)
(*                                                                         *)
(* Every cask key is an independent CASPaxos register. A write runs one    *)
(* Paxos round that agrees on the register's next value. This module       *)
(* abstracts a single register and proves the property every round relies  *)
(* on: AGREEMENT — at most one value is ever chosen per ballot, and every  *)
(* vote is "safe" (consistent with anything already choosable below it).   *)
(* This is the engine that makes each register update unambiguous.         *)
(*                                                                         *)
(* It is written in Lamport's abstract "voting" style (no message soup),   *)
(* which is the most directly model-checkable formulation of Paxos safety. *)
(* The MVCC value chain and HLC timestamp ride *inside* the agreed value   *)
(* and do not weaken this argument: per-key version/HLC monotonicity is a  *)
(* consequence of (a) this agreement and (b) the proposer always stamping  *)
(* strictly after the carried head (see internal/mvcc). A full refinement  *)
(* to register linearizability is future work tracked in tla/README.md.    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptor,  \* the set of acceptors
          Value,     \* the set of possible register values
          MaxBal     \* highest ballot number explored by the model

Ballot == 0..MaxBal

\* Majority quorums: any two majorities of a finite acceptor set intersect.
Quorum == {Q \in SUBSET Acceptor : 2 * Cardinality(Q) > Cardinality(Acceptor)}

ASSUME QuorumsIntersect == \A Q1, Q2 \in Quorum : Q1 \cap Q2 # {}

VARIABLES votes,   \* votes[a]  = set of <<ballot, value>> acceptor a has voted for
          maxBal   \* maxBal[a] = highest ballot a will still participate in (-1 = none)

vars == <<votes, maxBal>>

TypeOK ==
  /\ votes  \in [Acceptor -> SUBSET (Ballot \X Value)]
  /\ maxBal \in [Acceptor -> {-1} \cup Ballot]

Init ==
  /\ votes  = [a \in Acceptor |-> {}]
  /\ maxBal = [a \in Acceptor |-> -1]

VotedFor(a, b, v) == <<b, v>> \in votes[a]

\* A value is chosen at ballot b once a whole quorum has voted for it there.
ChosenAt(b, v) == \E Q \in Quorum : \A a \in Q : VotedFor(a, b, v)
Chosen(v)      == \E b \in Ballot : ChosenAt(b, v)

DidNotVoteAt(a, b) == \A v \in Value : ~VotedFor(a, b, v)

\* a can never vote at b in the future (it has moved past b without voting).
CannotVoteAt(a, b) == /\ maxBal[a] > b
                      /\ DidNotVoteAt(a, b)

\* No value other than v can still become chosen at ballot b.
NoneOtherChoosableAt(b, v) ==
  \E Q \in Quorum : \A a \in Q : VotedFor(a, b, v) \/ CannotVoteAt(a, b)

\* v is safe at b if, for every lower ballot, no other value is still choosable.
SafeAt(b, v) == \A c \in 0..(b - 1) : NoneOtherChoosableAt(c, v)

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

IncreaseMaxBal(a, b) ==
  /\ b > maxBal[a]
  /\ maxBal' = [maxBal EXCEPT ![a] = b]
  /\ UNCHANGED votes

VoteFor(a, b, v) ==
  /\ maxBal[a] =< b
  /\ \A vt \in votes[a] : vt[1] # b                          \* a hasn't voted at b
  /\ \A c \in Acceptor \ {a} :
       \A w \in Value : VotedFor(c, b, w) => (w = v)         \* one value per ballot
  /\ SafeAt(b, v)                                            \* carry-forward safety
  /\ votes'  = [votes  EXCEPT ![a] = @ \cup {<<b, v>>}]
  /\ maxBal' = [maxBal EXCEPT ![a] = b]

Next ==
  \/ \E a \in Acceptor, b \in Ballot : IncreaseMaxBal(a, b)
  \/ \E a \in Acceptor, b \in Ballot, v \in Value : VoteFor(a, b, v)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants — these are what TLC checks.                                 *)
(***************************************************************************)

\* No acceptor votes for two different values at the same ballot.
OneVotePerBallot ==
  \A a \in Acceptor, b \in Ballot, v, w \in Value :
    VotedFor(a, b, v) /\ VotedFor(a, b, w) => (v = w)

\* The whole system agrees on at most one value per ballot.
OneValuePerBallot ==
  \A a1, a2 \in Acceptor, b \in Ballot, v1, v2 \in Value :
    VotedFor(a1, b, v1) /\ VotedFor(a2, b, v2) => (v1 = v2)

\* Every vote is safe (the inductive heart of the agreement proof).
VotesSafe ==
  \A a \in Acceptor, b \in Ballot, v \in Value :
    VotedFor(a, b, v) => SafeAt(b, v)

\* AGREEMENT: once a value is chosen, no different value is ever chosen.
Consistency ==
  \A v, w \in Value : (Chosen(v) /\ Chosen(w)) => (v = w)

Inv ==
  /\ TypeOK
  /\ OneVotePerBallot
  /\ OneValuePerBallot
  /\ VotesSafe
  /\ Consistency

THEOREM Spec => []Inv
================================================================================
