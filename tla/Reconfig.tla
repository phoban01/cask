------------------------------- MODULE Reconfig -------------------------------
(***************************************************************************)
(* Safety specification for per-range reconfiguration under elastic churn  *)
(* — the M5 risk retired on paper before the driver is trusted.            *)
(*                                                                         *)
(* A range's replica set changes from Cold to Cnew via an intermediate     *)
(* JOINT configuration. The rule that makes this safe: while joint, a value *)
(* is chosen only with a quorum in BOTH Cold and Cnew. Because a joint      *)
(* quorum overlaps every Cold quorum and every Cnew quorum, and because a   *)
(* value already chosen is carried forward (catch-up) before Cold is        *)
(* released, agreement is preserved across the whole transition: no         *)
(* committed value is ever lost or contradicted.                           *)
(*                                                                         *)
(* This is the joint-consensus argument (as in Raft/CASPaxos membership     *)
(* change), specialised to a single register. It abstracts ballots — the    *)
(* per-ballot agreement is proved in CasPaxosMvcc.tla; here we model which   *)
(* values can become *chosen* in each configuration and show they cannot    *)
(* diverge.                                                                 *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptor, Value, Cold, Cnew

ASSUME ConfigsAreAcceptors == (Cold \subseteq Acceptor) /\ (Cnew \subseteq Acceptor)

\* Majority quorums of a configuration.
Quorums(C) == {Q \in SUBSET C : 2 * Cardinality(Q) > Cardinality(C)}

VARIABLES
  phase,     \* "old" -> "joint" -> "new": the reconfiguration stage
  voted,     \* voted[a] = set of values acceptor a has voted for
  oldChosen  \* snapshot at release: the values chosen in Cold when LeaveJoint ran

vars == <<phase, voted, oldChosen>>

TypeOK ==
  /\ phase \in {"old", "joint", "new"}
  /\ voted \in [Acceptor -> SUBSET Value]
  /\ oldChosen \subseteq Value

Init ==
  /\ phase = "old"
  /\ voted = [a \in Acceptor |-> {}]
  /\ oldChosen = {}

\* The set of configurations whose quorums are required to choose, given phase.
RequiredConfigs ==
  CASE phase = "old"   -> {Cold}
    [] phase = "joint" -> {Cold, Cnew}
    [] phase = "new"   -> {Cnew}

\* v is choosable now if every required configuration has a quorum that has all
\* voted for v.
ChosenNow(v) ==
  \A C \in RequiredConfigs :
    \E Q \in Quorums(C) : \A a \in Q : v \in voted[a]

\* The historical "chosen" predicate: a value chosen under any configuration whose
\* quorum has fully voted for it (old, new, or both).
ChosenIn(v, C) == \E Q \in Quorums(C) : \A a \in Q : v \in voted[a]
Chosen(v) == ChosenIn(v, Cold) \/ ChosenIn(v, Cnew)

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* An acceptor in an active configuration votes for v, but only if v is the
\* unique value any acceptor in the active configs has voted for (this models
\* the per-ballot agreement that CasPaxosMvcc.tla establishes: at the chosen
\* ballot all voters agree on one value, and carry-forward keeps it stable).
ActiveAcceptor == UNION RequiredConfigs
Vote(a, v) ==
  /\ a \in ActiveAcceptor
  /\ \A b \in ActiveAcceptor : \A w \in voted[b] : w = v
  /\ voted' = [voted EXCEPT ![a] = @ \cup {v}]
  /\ UNCHANGED <<phase, oldChosen>>

\* Enter the joint configuration.
EnterJoint ==
  /\ phase = "old"
  /\ phase' = "joint"
  /\ UNCHANGED <<voted, oldChosen>>

\* Leave the joint configuration for Cnew — only permitted once every value that
\* is chosen has been carried into Cnew (catch-up before release). This is the
\* load-bearing precondition. The values chosen in Cold are snapshotted at this
\* moment: CatchUpHeld must not evaluate ChosenIn(_, Cold) retroactively, since
\* a post-release vote by a shared acceptor can complete a Cold quorum that was
\* never commit-capable (Cold quorums stop being an authority at release; in
\* the implementation this is enforced by ballots, which this spec abstracts).
LeaveJoint ==
  /\ phase = "joint"
  /\ \A v \in Value : ChosenIn(v, Cold) => ChosenIn(v, Cnew)
  /\ oldChosen' = {v \in Value : ChosenIn(v, Cold)}
  /\ phase' = "new"
  /\ UNCHANGED voted

Next ==
  \/ \E a \in Acceptor, v \in Value : Vote(a, v)
  \/ EnterJoint
  \/ LeaveJoint

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* AGREEMENT ACROSS RECONFIGURATION: never two different chosen values.
NoLostValue == \A v, w \in Value : (Chosen(v) /\ Chosen(w)) => (v = w)

\* Once in Cnew, anything that was chosen in Cold at the moment of release is
\* still chosen in Cnew (evaluated against the release-time snapshot, not
\* retroactively — see LeaveJoint).
CatchUpHeld ==
  (phase = "new") => \A v \in oldChosen : ChosenIn(v, Cnew)

Inv == TypeOK /\ NoLostValue /\ CatchUpHeld

THEOREM Spec => []Inv
================================================================================
