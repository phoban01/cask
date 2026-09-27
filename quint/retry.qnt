// The retry contract for one client write (docs/spec/fleet.md §3).
//
// A cask write runs CASPaxos. If its accept phase reaches only a minority,
// the proposer sees a failure, but a later prepare can still adopt and
// choose its value. So a write that returned a conflict may have landed.
//
// The model has one register and one client with one logical write.
// `applied` counts how many times that write took effect. The invariant is
// that it takes effect at most once. Retrying with a compare-and-set keeps
// the invariant. Blindly reapplying the change, which is what a
// non-idempotent CASPaxos change function does on retry, breaks it.
//
// The outcome of an unknown write is fixed when the client re-reads,
// because the re-read is itself a consensus round: it either adopts the
// value or supersedes it. So the model picks the outcome at the attempt.
module retry {
  type Status = Idle | Unknown | Done

  var seq: int        // register sequence; the resourceVersion
  var applied: int    // times the client's write took effect
  var clientRV: int   // the resourceVersion the client last read
  var status: Status

  action init = all {
    seq' = 0, applied' = 0, clientRV' = 0, status' = Idle,
  }

  // Another writer changes the register.
  action otherWrite = all {
    seq' = seq + 1,
    applied' = applied, clientRV' = clientRV, status' = status,
  }

  // The client's compare-and-set reaches a majority.
  action casCommits = all {
    status != Done,
    seq == clientRV,
    seq' = seq + 1, applied' = applied + 1, clientRV' = clientRV,
    status' = Done,
  }

  //= docs/spec/fleet.md#3-storage-model
  //# A write that returned a conflict MAY have been committed.
  // The accept reaches a minority. The client sees a conflict, and the
  // value may or may not be chosen later.
  action casUnknownWith(chosen: bool): bool = all {
    status != Done,
    seq == clientRV,
    seq' = if (chosen) seq + 1 else seq,
    applied' = if (chosen) applied + 1 else applied,
    clientRV' = clientRV, status' = Unknown,
  }
  action casUnknown = { nondet chosen = oneOf(Set(true, false)) casUnknownWith(chosen) }

  // The compare-and-set loses to another writer.
  action casConflict = all {
    status != Done,
    seq != clientRV,
    seq' = seq, applied' = applied, clientRV' = clientRV, status' = Unknown,
  }

  //= docs/spec/fleet.md#3-storage-model
  //# The extension server MUST re-read an object before it retries a write
  //# that returned a conflict.
  // After a re-read the client sees its own write if it landed, and stops.
  action reread = all {
    status == Unknown,
    clientRV' = seq,
    status' = if (applied > 0) Done else Idle,
    seq' = seq, applied' = applied,
  }

  //= docs/spec/fleet.md#3-storage-model
  //# A retried write MUST be a compare-and-set, never a blind reapplication
  //# of a change.
  action step = any { otherWrite, casCommits, casUnknown, casConflict, reread }

  val AppliedAtMostOnce = applied <= 1

  // ---- negative control: retry by reapplying the change ----------------------

  // The proposer's own retry loop: re-read the current value inside the
  // next round and apply the change again, with no compare-and-set.
  action blindRetry = all {
    status == Unknown,
    seq' = seq + 1, applied' = applied + 1, clientRV' = clientRV,
    status' = Done,
  }
  action stepBlindRetry = any { otherWrite, casCommits, casUnknown, casConflict, blindRetry }

  // ---- witness runs ------------------------------------------------------------

  // The write lands although the client saw a conflict. The re-read finds it.
  run conflictButCommittedTest =
    init
      .then(casUnknownWith(true))
      .expect(applied == 1 and status == Unknown)
      .then(reread)
      .expect(status == Done and applied == 1)
}
