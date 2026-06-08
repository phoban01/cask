--------------------------- MODULE RangeDescriptors ---------------------------
(***************************************************************************)
(* Safety specification for the cask range-descriptor split protocol       *)
(* (etcd-little-sister §4.3 design C', Variant 1: descriptors-first,       *)
(* roster-cutover).                                                        *)
(*                                                                         *)
(* A split of range R_old produces two new ranges, L and R, that together  *)
(* cover R_old's keyspace. The protocol is four CASPaxos commits on the    *)
(* Core, in order:                                                         *)
(*                                                                         *)
(*   1. CreateL    write descriptor L (Live)                                *)
(*   2. CreateR    write descriptor R (Live)                                *)
(*   3. Cutover    atomically swap the roster's RangeIDs (R_old -> {L,R})  *)
(*   4. Tombstone  mark R_old's descriptor as not-Live                      *)
(*                                                                         *)
(* This spec models a single key K whose authoritative range flips from    *)
(* R_old (here: "O") to L at Cutover. The proof generalises by symmetry to *)
(* every key in the split region.                                          *)
(*                                                                         *)
(* The safety invariant: a client write is only accepted if the client's   *)
(* believed authoritative range matches the protocol's current             *)
(* authoritative range (i.e. the proposer's descriptor-epoch check at      *)
(* §4.1 rejects stale beliefs with ErrRangeChanged). Therefore no client   *)
(* with a stale belief ever successfully writes to the now-old range. This *)
(* is what makes the rmap-cache + ErrRangeChanged pattern safe.            *)
(*                                                                         *)
(* The per-register CASPaxos engine under each descriptor is proved        *)
(* separately by CasPaxosMvcc.tla; reconfig of a range's replica set is    *)
(* proved by Reconfig.tla. This spec is about the *cross-register*         *)
(* sequencing of the split protocol — the part that is novel to §4.3.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS Client

\* Three range identities for the split: O (old), L (new left), R (new right).
Range == {"O", "L", "R"}

\* Phases of the four-step protocol.
Phase == {"initial", "createdL", "createdR", "cutover", "tombstoned"}

\* The authoritative range for the key, as a function of the protocol phase.
\* Before Cutover, the roster still names O; from Cutover onward, L is
\* authoritative for this key. (R is authoritative for the other half of the
\* split region; modelled by symmetry.)
Authoritative(p) ==
  IF p \in {"initial", "createdL", "createdR"} THEN "O" ELSE "L"

VARIABLES
  phase,        \* current protocol phase
  exists,       \* exists[r] = TRUE iff descriptor r has any value (Live or not)
  alive,        \* alive[r]  = TRUE iff descriptor r is Live (accepts writes)
  believed,     \* believed[c] = range client c believes is authoritative for K
  writes        \* set of <<client, range, phase>> — accepted client writes

vars == <<phase, exists, alive, believed, writes>>

TypeOK ==
  /\ phase    \in Phase
  /\ exists   \in [Range -> BOOLEAN]
  /\ alive    \in [Range -> BOOLEAN]
  /\ believed \in [Client -> Range]
  /\ writes   \subseteq (Client \X Range \X Phase)

Init ==
  /\ phase    = "initial"
  /\ exists   = [r \in Range |-> r = "O"]
  /\ alive    = [r \in Range |-> r = "O"]
  /\ believed = [c \in Client |-> "O"]
  /\ writes   = {}

(***************************************************************************)
(* Protocol actions — the four split steps.                                *)
(***************************************************************************)

CreateL ==
  /\ phase = "initial"
  /\ phase'  = "createdL"
  /\ exists' = [exists EXCEPT !["L"] = TRUE]
  /\ alive'  = [alive  EXCEPT !["L"] = TRUE]
  /\ UNCHANGED <<believed, writes>>

CreateR ==
  /\ phase = "createdL"
  /\ phase'  = "createdR"
  /\ exists' = [exists EXCEPT !["R"] = TRUE]
  /\ alive'  = [alive  EXCEPT !["R"] = TRUE]
  /\ UNCHANGED <<believed, writes>>

\* The atomic cutover commit on the roster: Authoritative(phase) flips here.
Cutover ==
  /\ phase = "createdR"
  /\ phase' = "cutover"
  /\ UNCHANGED <<exists, alive, believed, writes>>

Tombstone ==
  /\ phase = "cutover"
  /\ phase' = "tombstoned"
  /\ alive' = [alive EXCEPT !["O"] = FALSE]
  /\ UNCHANGED <<exists, believed, writes>>

(***************************************************************************)
(* Client actions.                                                         *)
(***************************************************************************)

\* A client refreshes its rmap to the current authoritative range. Models the
\* rmap-refresh after an ErrRangeChanged or a periodic poll. May happen at any
\* phase; weak fairness drives liveness (L1).
RefreshBelief(c) ==
  /\ believed[c] # Authoritative(phase)
  /\ believed' = [believed EXCEPT ![c] = Authoritative(phase)]
  /\ UNCHANGED <<phase, exists, alive, writes>>

\* A client attempts a write of K to its believed range. The proposer accepts
\* iff (a) the believed range matches the current authoritative range — this
\* is the descriptor-epoch check that returns ErrRangeChanged on mismatch —
\* and (b) the descriptor is Live (not Tombstoned). Stale-belief writes are
\* simply disabled (no successful commit).
ClientWrite(c) ==
  /\ believed[c] = Authoritative(phase)
  /\ alive[believed[c]]
  /\ writes' = writes \cup {<<c, believed[c], phase>>}
  /\ UNCHANGED <<phase, exists, alive, believed>>

Next ==
  \/ CreateL \/ CreateR \/ Cutover \/ Tombstone
  \/ \E c \in Client : RefreshBelief(c)
  \/ \E c \in Client : ClientWrite(c)

\* Weak fairness on RefreshBelief drives L1: eventually every client catches
\* up to the current authoritative range.
Fairness == \A c \in Client : WF_vars(RefreshBelief(c))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* S8: NoSplitBrain — every accepted write targets the authoritative range
\* as of the phase the write was accepted in. The contrapositive of "a
\* client with a stale belief never successfully writes to the now-old
\* range" is exactly this: no <<c, r, p>> ever lands in writes with
\* r # Authoritative(p).
NoSplitBrain ==
  \A w \in writes : w[2] = Authoritative(w[3])

\* The authoritative range for K is always materialised and alive. (If this
\* fails, the cutover step would be observably broken.)
AuthorityExistsAndLive ==
  /\ exists[Authoritative(phase)]
  /\ alive[Authoritative(phase)]

\* S9 (descriptor-epoch carry-forward) — once a phase advances past
\* tombstoning, O is no longer accepting writes. Captured here as a
\* monotonicity invariant on alive["O"]: once false, stays false.
\* (Inductive form; tombstone is the only action that flips alive["O"]
\* to false, and no action flips it back to true.)
TombstoneIsTerminal ==
  (phase = "tombstoned") => ~alive["O"]

Inv ==
  /\ TypeOK
  /\ NoSplitBrain
  /\ AuthorityExistsAndLive
  /\ TombstoneIsTerminal

(***************************************************************************)
(* Liveness (L1) — under fairness, every client eventually believes the    *)
(* authoritative range. This is what makes the rmap-cache pattern useful:  *)
(* not just safe (NoSplitBrain) but also live.                             *)
(***************************************************************************)

EventuallyCaughtUp ==
  <>[](\A c \in Client : believed[c] = Authoritative(phase))

THEOREM Spec => []Inv
THEOREM Spec => EventuallyCaughtUp
================================================================================
