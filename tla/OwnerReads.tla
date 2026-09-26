------------------------------ MODULE OwnerReads ------------------------------
(***************************************************************************)
(* Safety spec for W4 lease-guarded owner reads                            *)
(* (docs/plans/quepaxa-learnings-implementation.md).                       *)
(*                                                                         *)
(* An owner serves reads from its local cache with no consensus round. A   *)
(* write detects deposition by its accept NACKing; a read detects nothing, *)
(* so validity must come from TIME: the double-sided MaxOffset guard.      *)
(* With Guarded = TRUE:                                                    *)
(*                                                                         *)
(*   owner side: serve only while localNow(c) + Skew < expiry              *)
(*   taker side: take over a lapsed holder only once                       *)
(*               localNow(taker) >= expiry + Skew                          *)
(*                                                                         *)
(* Under |clock offset| <= Skew, the last read the old owner can serve     *)
(* strictly precedes (in true time) the first write any successor can      *)
(* commit — NoStaleRead holds. OwnerReadsBug.cfg is the negative control   *)
(* (Guarded = FALSE, the naive lapsed-check on both sides): TLC MUST find  *)
(* a stale read served by an owner whose slow clock still shows a live     *)
(* lease while a fast-clocked successor has already taken over and         *)
(* written.                                                                *)
(*                                                                         *)
(* Scope (deliberate): writes go only through the current lock holder —    *)
(* the writes-via-owner discipline. Cask's interim full-path side door     *)
(* (pre-M7 forwarding) is outside this spec and documented as a gap in     *)
(* internal/owner; epoch fencing keeps WRITES safe there regardless, and   *)
(* the router invalidates the cache for the fallbacks it carries.          *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS Client,    \* the candidate owners
          NoClient,  \* model value: the "nobody" holder
          Skew,      \* assumed bound on |clock offset| (MaxOffset)
          TTL,       \* lease duration
          MaxTime,   \* true-time horizon
          MaxWrites, \* register write budget (bounds the model)
          Guarded    \* TRUE = the W4 double-sided guard; FALSE = naive

ASSUME NoClient \notin Client

VARIABLES
  now,     \* true time (no process sees it directly)
  off,     \* off[c]: c's fixed clock offset, |off[c]| <= Skew (adversarial)
  holder,  \* current lock holder (NoClient if none)
  sexp,    \* the stored lease expiry (written via consensus at grant time)
  gexp,    \* gexp[c]: the expiry from c's own last grant (its local knowledge)
  regVal,  \* the register's committed value counter
  cache,   \* cache[c]: what c's fast-path cache holds
  bad      \* TRUE iff some served local read returned a non-latest value

vars == <<now, off, holder, sexp, gexp, regVal, cache, bad>>

LocalNow(c) == now + off[c]

Init ==
  /\ now = 0
  /\ off \in [Client -> -Skew..Skew]
  /\ holder = NoClient
  /\ sexp = 0
  /\ gexp = [c \in Client |-> 0]
  /\ regVal = 0
  /\ cache = [c \in Client |-> 0]
  /\ bad = FALSE

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<off, holder, sexp, gexp, regVal, cache, bad>>

\* The lock looks free to c: never granted, or lapsed on c's clock — plus the
\* taker-side wait margin when guarded.
TakeableBy(c) ==
  \/ holder = NoClient /\ sexp = 0
  \/ IF Guarded THEN LocalNow(c) >= sexp + Skew ELSE LocalNow(c) >= sexp

\* Acquire: c takes the lock, records the expiry ON ITS OWN CLOCK (that is
\* what Sessions.Grant commits), and primes its cache from a phase-1 read.
Acquire(c) ==
  /\ TakeableBy(c)
  /\ holder' = c
  /\ sexp' = LocalNow(c) + TTL
  /\ gexp' = [gexp EXCEPT ![c] = LocalNow(c) + TTL]
  /\ cache' = [cache EXCEPT ![c] = regVal]
  /\ UNCHANGED <<now, off, regVal, bad>>

\* Write: only the current holder commits (writes-via-owner discipline; a
\* deposed owner's write is epoch-fenced in the implementation and never
\* lands). The writer's own cache tracks its write.
Write(c) ==
  /\ holder = c
  /\ regVal < MaxWrites
  /\ regVal' = regVal + 1
  /\ cache' = [cache EXCEPT ![c] = regVal + 1]
  /\ UNCHANGED <<now, off, holder, sexp, gexp, bad>>

\* CanServe: c believes its lease covers reads, judged ONLY from its local
\* clock and its own granted expiry — c has no way to see the successor.
CanServe(c) ==
  /\ gexp[c] > 0
  /\ IF Guarded THEN LocalNow(c) + Skew < gexp[c] ELSE LocalNow(c) < gexp[c]

\* LocalRead: c serves its cache with no consensus round. Stale = the cache
\* is not the latest committed value at this true instant.
LocalRead(c) ==
  /\ CanServe(c)
  /\ bad' = (bad \/ cache[c] # regVal)
  /\ UNCHANGED <<now, off, holder, sexp, gexp, regVal, cache>>

Next ==
  \/ Tick
  \/ \E c \in Client : Acquire(c)
  \/ \E c \in Client : Write(c)
  \/ \E c \in Client : LocalRead(c)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* Every served local read returned the latest committed value.
NoStaleRead == ~bad

TypeOK ==
  /\ now \in 0..MaxTime
  /\ holder \in Client \cup {NoClient}
  /\ regVal \in 0..MaxWrites
  /\ bad \in BOOLEAN

Inv ==
  /\ TypeOK
  /\ NoStaleRead

THEOREM Spec => []Inv
================================================================================
