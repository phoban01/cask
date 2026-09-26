--------------------------------- MODULE Lease ---------------------------------
(***************************************************************************)
(* Safety specification for cask leases/locks.                             *)
(*                                                                         *)
(* A lease is a CASPaxos register holding {owner, expiry, fence}. Time is  *)
(* modelled as a monotonic counter `now`; a lease is live while its expiry *)
(* is in the future. The two safety properties cask relies on for locks:   *)
(*                                                                         *)
(*   SingleHolder  — at most one client holds a live lease at any instant. *)
(*   FenceMonotone — the fencing token strictly increases on every grant,  *)
(*                   and the live holder always carries the latest token,  *)
(*                   so a fenced resource can reject a stale/zombie holder. *)
(*                                                                         *)
(* Expiry here is "lazy": acquisition is permitted exactly when no client  *)
(* currently holds a live lease, which models the lazy-expiry-on-access    *)
(* rule (an expired lease is never observed as live).                      *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS Client,   \* the set of clients that may take the lease
          MaxTime,  \* time horizon for the model
          TTL,      \* lease duration granted on acquire/renew
          MaxFence, \* fence horizon (Bump can jump, so time no longer bounds it)
          NoClient  \* model value: the "nobody" owner (an unbounded CHOOSE is not TLC-evaluable)

ASSUME NoClient \notin Client

VARIABLES owner,    \* the client currently granted the lease, or NoClient
          expiry,   \* absolute time the current grant expires
          fence,    \* global monotonic counter of grants issued
          held,     \* held[c]  = the fence token client c believes it holds (0 = none)
          now       \* current time

vars == <<owner, expiry, fence, held, now>>

TypeOK ==
  /\ owner  \in Client \cup {NoClient}
  /\ expiry \in 0..(MaxTime + TTL)
  /\ fence  \in 0..MaxFence
  /\ held   \in [Client -> 0..MaxFence]
  /\ now    \in 0..MaxTime

Init ==
  /\ owner  = NoClient
  /\ expiry = 0
  /\ fence  = 0
  /\ held   = [c \in Client |-> 0]
  /\ now    = 0

\* A live lease is one owned by a client whose grant has not yet expired.
Live == owner # NoClient /\ expiry > now

\* Acquire is enabled only when no live lease exists (lazy expiry). It mints a
\* strictly higher fence token and hands it to the acquirer.
Acquire(c) ==
  /\ ~Live
  /\ fence < MaxFence
  /\ fence'  = fence + 1
  /\ owner'  = c
  /\ expiry' = now + TTL
  /\ held'   = [held EXCEPT ![c] = fence + 1]
  /\ UNCHANGED now

\* The live holder raises its own fence, possibly by more than one — the
\* Locks.Bump operation (W0): when the consensus layer reports the register at
\* a higher epoch (a full proposer's synthetic epoch), the holder must jump
\* its fence past it without releasing. Ownership does not change and the
\* holder still carries the latest token, so SingleHolder and FenceLatest are
\* preserved; the jump width does not matter, only monotonicity.
Bump(c) ==
  /\ owner = c
  /\ Live
  /\ fence < MaxFence
  /\ \E f \in (fence + 1)..MaxFence :
       /\ fence' = f
       /\ held'  = [held EXCEPT ![c] = f]
  /\ UNCHANGED <<owner, expiry, now>>

\* The current holder may renew while still live, extending expiry. Renewal does
\* not change ownership; we leave the fence token unchanged on renew.
Renew(c) ==
  /\ owner = c
  /\ Live
  /\ expiry' = now + TTL
  /\ UNCHANGED <<owner, fence, held, now>>

\* Voluntary release by the current holder.
Release(c) ==
  /\ owner = c
  /\ owner'  = NoClient
  /\ held'   = [held EXCEPT ![c] = 0]
  /\ UNCHANGED <<expiry, fence, now>>

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<owner, expiry, fence, held>>

Next ==
  \/ \E c \in Client : Acquire(c)
  \/ \E c \in Client : Renew(c)
  \/ \E c \in Client : Bump(c)
  \/ \E c \in Client : Release(c)
  \/ Tick

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* At most one client holds a live lease. (Trivial structurally, but TLC
\* confirms no action sequence can produce a second live holder.)
SingleHolder ==
  \A c, d \in Client :
    (owner = c /\ expiry > now /\ owner = d) => (c = d)

\* The live holder always carries the most recently issued fence token, and no
\* client believes it holds a token greater than the number issued.
FenceLatest ==
  /\ (Live => held[owner] = fence)
  /\ \A c \in Client : held[c] =< fence

Inv ==
  /\ TypeOK
  /\ SingleHolder
  /\ FenceLatest

\* Fencing tokens never decrease across a step.
FenceMonotone == [][fence' >= fence]_vars

THEOREM Spec => []Inv
================================================================================
