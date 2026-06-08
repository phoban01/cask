------------------------------ MODULE CrossRange ------------------------------
(***************************************************************************)
(* Safety specification for cask's cross-range snapshot read contract      *)
(* (etcd-little-sister §4.4 design A: documented uncertainty window).      *)
(*                                                                         *)
(* Cask does not provide strict cross-range linearizability for snapshots. *)
(* Instead it documents a bounded uncertainty window: for any key that has *)
(* not been modified during [t - MaxOffset, t], a SnapshotRead at HLC      *)
(* timestamp t returns the unique latest pre-t committed value. Keys       *)
(* modified within the window may return any value committed in the       *)
(* window. This is the contract `mvcc.SnapshotRead` exposes.               *)
(*                                                                         *)
(* This spec models two ranges, each with its own HLC, drift bounded by   *)
(* MaxOffset. Writes commit at the writing range's local HLC.             *)
(* SnapshotReads read each key's range at a chosen `t`, returning the     *)
(* latest committed entry with hlc <= t.                                  *)
(*                                                                         *)
(* Cross-key consistency for coordination workloads is enforced elsewhere *)
(* by fencing tokens at write time (see Lease.tla), not by snapshot      *)
(* semantics. This spec proves the contract; it does not claim more.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
  Range,        \* the set of ranges
  Key,          \* the set of keys
  Value,        \* the set of values writes commit
  RangeOfKey,   \* function Key -> Range: which range hosts each key
  MaxOffset,    \* max HLC skew between any two ranges
  MaxTime       \* model horizon

ASSUME RangeOfKeyTyped == RangeOfKey \in [Key -> Range]
ASSUME MaxOffsetTyped  == MaxOffset \in Nat
ASSUME MaxTimeTyped    == MaxTime \in Nat /\ MaxTime >= MaxOffset

\* Sentinel returned by a read against an empty history.
NoValue == CHOOSE x : x \notin Value

VARIABLES
  hlc,          \* hlc[r] - the range's local HLC (monotonic counter)
  history,      \* history[k] - sequence of <<hlc_at_commit, value>>
  reads         \* set of <<t, k, returned_value, returned_hlc>>

vars == <<hlc, history, reads>>

TypeOK ==
  /\ hlc     \in [Range -> 0..MaxTime]
  /\ history \in [Key -> Seq((0..MaxTime) \X Value)]
  /\ reads   \subseteq ((0..MaxTime) \X Key \X (Value \cup {NoValue}) \X (0..MaxTime))

Init ==
  /\ hlc     = [r \in Range |-> 0]
  /\ history = [k \in Key |-> <<>>]
  /\ reads   = {}

\* Bounded skew: HLC values across ranges never drift more than MaxOffset.
SkewOK(h) ==
  \A r1, r2 \in Range :
    h[r1] - h[r2] <= MaxOffset /\ h[r2] - h[r1] <= MaxOffset

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* A range advances its HLC by 1 (idle tick). Guarded by the skew bound so
\* TickHLC can never produce a state that violates NoSkewOverflow.
TickHLC(r) ==
  /\ hlc[r] < MaxTime
  /\ \A r2 \in Range \ {r} : hlc[r] + 1 - hlc[r2] <= MaxOffset
  /\ hlc' = [hlc EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<history, reads>>

\* A write to key k of value v: the key's range advances its HLC, stamps the
\* new value, and appends to history.
Write(k, v) ==
  LET r == RangeOfKey[k] IN
  /\ hlc[r] < MaxTime
  /\ \A r2 \in Range \ {r} : hlc[r] + 1 - hlc[r2] <= MaxOffset
  /\ hlc'     = [hlc     EXCEPT ![r] = @ + 1]
  /\ history' = [history EXCEPT ![k] = Append(@, <<hlc[r] + 1, v>>)]
  /\ UNCHANGED reads

\* The latest committed entry of k with hlc <= t, or <<0, NoValue>> if none.
LatestAtTime(k, t) ==
  LET h == history[k]
      cands == {i \in 1..Len(h) : h[i][1] <= t}
  IN  IF cands = {}
      THEN <<0, NoValue>>
      ELSE LET m == CHOOSE i \in cands : \A j \in cands : h[i][1] >= h[j][1]
           IN  h[m]

\* A snapshot read of key k at time t. Returns the per-range latest pre-t.
SnapshotRead(t, k) ==
  /\ LET pair == LatestAtTime(k, t) IN
     reads' = reads \cup {<<t, k, pair[2], pair[1]>>}
  /\ UNCHANGED <<hlc, history>>

Next ==
  \/ \E r \in Range : TickHLC(r)
  \/ \E k \in Key, v \in Value : Write(k, v)
  \/ \E t \in 0..MaxTime, k \in Key : SnapshotRead(t, k)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* S10a: NoSkewOverflow — HLC skew across ranges stays within MaxOffset.
\* Structurally enforced by the TickHLC and Write guards; this invariant
\* documents the contract.
NoSkewOverflow == SkewOK(hlc)

\* S10b: UncertaintyContract — for any snapshot read whose key was *not*
\* modified inside the uncertainty window [t - MaxOffset, t], the returned
\* value is the unique latest committed entry with hlc <= t. Reads inside
\* the window are unconstrained (the contract is silent on them).
UncertaintyContract ==
  \A read \in reads :
    LET t            == read[1]
        k            == read[2]
        returned_v   == read[3]
        returned_hlc == read[4]
        h            == history[k]
        OutsideWindow ==
          \A i \in 1..Len(h) : h[i][1] < t - MaxOffset \/ h[i][1] > t
        cands == {i \in 1..Len(h) : h[i][1] <= t}
    IN  OutsideWindow =>
        IF cands = {}
        THEN returned_v = NoValue /\ returned_hlc = 0
        ELSE \E i \in cands :
               /\ h[i][1] = returned_hlc
               /\ h[i][2] = returned_v
               /\ \A j \in cands : h[j][1] <= h[i][1]

Inv ==
  /\ TypeOK
  /\ NoSkewOverflow
  /\ UncertaintyContract

THEOREM Spec => []Inv
================================================================================
