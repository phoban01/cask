----------------------------- MODULE RosterReconfig -----------------------------
(***************************************************************************)
(* Safety of REFLEXIVE roster reconfiguration.                             *)
(*                                                                         *)
(* cask's membership register stores its OWN acceptor set (the "core"), and *)
(* that core is changed old -> joint -> new by joint consensus run ON THE   *)
(* REGISTER ITSELF, while membership writes (joins and departures) keep     *)
(* committing concurrently. Reconfig.tla proves the joint-quorum overlap    *)
(* for a single static value; the reflexive case is harder because the      *)
(* value ADVANCES during the very transition that hands the register to a   *)
(* new quorum.                                                              *)
(*                                                                         *)
(* We model the membership value as a monotonically increasing version. A   *)
(* version is "chosen" when a quorum of an active configuration holds it. We *)
(* show that handing the register to a new core never loses a committed     *)
(* membership: the carry-forward precondition on leaving the joint phase     *)
(* guarantees that, once the new core is released, it holds the latest       *)
(* committed version — nothing written under the old core is dropped.        *)
(*                                                                         *)
(* Ballots are abstracted (proved per-ballot in CasPaxosMvcc.tla); here we   *)
(* model which versions can become chosen in each configuration.            *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Node, Cold, Cnew, MaxVer

ASSUME ConfigsOK == /\ Cold \subseteq Node
                    /\ Cnew \subseteq Node
                    /\ MaxVer \in Nat

\* Majority quorums of a configuration.
Quorums(C) == {Q \in SUBSET C : 2 * Cardinality(Q) > Cardinality(C)}

NoVer == -1            \* an acceptor that holds no membership value yet
Vers  == 0..MaxVer

VARIABLES
  phase,   \* "old" -> "joint" -> "new": the core reconfiguration stage
  ver      \* ver[a] = highest membership version acceptor a holds (NoVer if none)

vars == <<phase, ver>>

TypeOK ==
  /\ phase \in {"old", "joint", "new"}
  /\ ver \in [Node -> {NoVer} \cup Vers]

\* Genesis: the old core holds membership version 0; nobody else has anything.
Init ==
  /\ phase = "old"
  /\ ver = [a \in Node |-> IF a \in Cold THEN 0 ELSE NoVer]

\* The configurations whose quorums are required to choose, given the phase.
RequiredConfigs ==
  CASE phase = "old"   -> {Cold}
    [] phase = "joint" -> {Cold, Cnew}
    [] phase = "new"   -> {Cnew}

\* Version k is chosen in C when some quorum of C all hold at least k.
ChosenIn(k, C) == \E Q \in Quorums(C) : \A a \in Q : ver[a] >= k
Chosen(k)      == ChosenIn(k, Cold) \/ ChosenIn(k, Cnew)

\* The greatest chosen version (0 is always chosen, so this is well defined).
MaxChosen == CHOOSE k \in Vers : Chosen(k) /\ \A j \in Vers : Chosen(j) => j <= k

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* A membership write (a join or departure) commits the next version under a
\* quorum of EVERY required configuration — a single quorum while quiescent, a
\* joint quorum (both cores) mid-reconfiguration. Versions are monotonic: an
\* acceptor only ever raises the version it holds.
Raise(S, k) == [a \in Node |-> IF a \in S THEN k ELSE ver[a]]

Write ==
  /\ MaxChosen < MaxVer
  /\ LET k == MaxChosen + 1 IN
       \E Qo \in Quorums(Cold), Qn \in Quorums(Cnew) :
         \/ (phase = "old"   /\ ver' = Raise(Qo, k))
         \/ (phase = "joint" /\ ver' = Raise(Qo \cup Qn, k))
         \/ (phase = "new"   /\ ver' = Raise(Qn, k))
  /\ UNCHANGED phase

\* Begin moving the core: now every write needs a joint quorum.
EnterJoint ==
  /\ phase = "old"
  /\ phase' = "joint"
  /\ UNCHANGED ver

\* Release the old core. The LOAD-BEARING precondition (catch-up before release):
\* the latest committed version must already be present in the new core, so no
\* membership committed under the old core is lost when it is dropped.
LeaveJoint ==
  /\ phase = "joint"
  /\ ChosenIn(MaxChosen, Cnew)
  /\ phase' = "new"
  /\ UNCHANGED ver

Next ==
  \/ Write
  \/ EnterJoint
  \/ LeaveJoint

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* NO LOST MEMBERSHIP: once the register has been handed to the new core, that
\* core holds the latest committed membership version. Combined with the fact
\* that chosen versions are never un-chosen (ver only rises), this is exactly
\* "the core handoff loses no committed value".
NoLostMembership == (phase = "new") => ChosenIn(MaxChosen, Cnew)

\* The latest committed membership is always available on some active config —
\* the register is never stranded on a quorum we have left behind.
AlwaysAvailable == \E C \in RequiredConfigs : ChosenIn(MaxChosen, C)

Inv == TypeOK /\ NoLostMembership /\ AlwaysAvailable

THEOREM Spec => []Inv
================================================================================
