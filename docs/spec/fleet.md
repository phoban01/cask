# Cask fleet objects

Cask serves cluster-scoped Kubernetes resources from every management
cluster in a fleet. The objects live in cask, embedded in the extension
API server of each cluster. Consumers see ordinary Kubernetes resources.

This document is the requirement source. Duvet extracts every sentence
that contains a requirement keyword and tracks its citations in code and
tests. The Quint model in `quint/fleet.qnt` checks the safety rules in
sections 3 and 5. The plan and the reasoning live in the conversation
that produced this document and in `docs/k8s-aggregation.md`.

Terms:

- Fleet: the set of management clusters that share fleet objects.
- Member: one extension API server process in the fleet.
- Voter: a member that holds register replicas and votes in consensus.
- Participant: a member that reads, watches, and proposes but holds no replicas.
- Object register: the cask register that stores one object.
- Index register: the cask register that lists every object of one resource type.
- Fence: the monotonic token that a lock acquisition mints.
- Effect: any write that a controller makes on behalf of a claim.

## 2. Resources

Cask MUST serve its resources through the Kubernetes aggregation layer as an APIService.

Every fleet resource MUST be cluster-scoped.

The extension server MUST be built on the generic server in k8s.io/apiserver.

The extension server MUST delegate authentication and authorization to the local kube-apiserver.

A client MUST be able to use kubectl, client-go informers, field selectors, and watch bookmarks against fleet resources without fleet-specific code.

The extension server MUST report not ready until its storage is reachable.

The extension server MUST report not ready until any pending migration import is complete.

## 3. Storage model

Each object MUST be stored in one cask register keyed by resource type and name.

Each resource type MUST have one index register that maps every object name to that object's latest sequence.

An object's resourceVersion MUST be the sequence of its object register.

A list's resourceVersion MUST be the sequence of the index register.

A mutation MUST write the object register before the index register.

The index register MUST NOT record a sequence higher than the object register holds.

When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.

The extension server MUST reconcile the index register against the object registers at startup.

The extension server MUST reconcile the index register against the object registers at a fixed interval.

A create MUST use a compare-and-set that requires the object register to be absent.

An update MUST use a compare-and-set on the resourceVersion the client supplied.

An update whose compare-and-set fails MUST return a conflict.

A write that returned a conflict MAY have been committed.

The extension server MUST re-read an object before it retries a write that returned a conflict.

A retried write MUST be a compare-and-set, never a blind reapplication of a change.

A delete MUST tombstone the object register before it removes the name from the index register.

An object value MUST NOT exceed 1 MiB.

## 4. List and watch

A list MUST return every object that the index register names at the index sequence the list reports.

A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.

A watch whose start version is compacted MUST end with 410 Gone.

Each watch event MUST carry the object at the sequence the index recorded.

Watch events SHOULD be pushed from the index register's change feed rather than polled.

## 5. Claims and fencing

Binding a claim to an object MUST be the acquisition of that object's cask lock under the claim's session.

At most one claim MUST be Bound to an object at the object's current fence.

Every successful acquisition MUST mint a fence strictly greater than every fence previously minted for that object.

An acquire by the session that already holds the lock MUST return the current fence and MUST NOT mint a new one.

A Bound claim MUST carry its fence in its status.

A controller MUST include the claim's fence in every effect it applies for that object.

A receiver MUST reject an effect whose fence is lower than the highest fence it has accepted for that object.

A status write MUST NOT lower an advertised fence.

A controller MUST reconcile only claims whose status names its own cluster.

When a claim's session lapses, the controller MUST set the claim to Lost.

A controller MUST NOT apply an effect for a claim it knows to be Lost.

Deleting a Bound claim MUST release the object's lock.

Fencing rejects effects that are older than the newest holder's fence; it MAY accept an expired holder's effect while no successor exists.

## 6. Membership

A fleet MUST start from exactly one founding member.

Every member MUST be either a voter or a participant.

A participant MUST be able to read, watch, and propose without holding register replicas.

Only voters MUST hold register replicas.

The voter set MUST change only by joint-consensus reconfiguration of the roster.

A core change MUST carry every data register forward to the new core before it releases the old core.

During a core change, a data write MUST reach a quorum of both the old and the new core.

A voter MUST reject a data write that names an older core configuration than the roster value it has accepted.

A voter MUST NOT list its data keys while a data write that passed its fence is still in progress.

A core change MUST list data keys only on old voters that have accepted the joint roster value.

A core change MUST finish while a majority of the old core and a majority of the new core answer.

A resumed core change MUST carry every data register that it did not carry under the same joint configuration.

The extension server MUST answer a data write that a voter rejected as stale with a retryable status.

An operator MUST NOT grow the voter set by restarting members with a longer static peer list.

The voter count MUST be odd.

The voter count MUST NOT exceed five.

The voter set SHOULD contain at most one voter per management cluster once three or more clusters exist.

The founding member MUST remain available until the voter set has grown to three.

The extension server MUST join the fleet through the dynamic roster path.

A new member MUST be able to join with a seed address and a minted certificate and nothing else.

## 7. Migration

The extension server MUST serve the same API group, version, and kinds that the CRD served.

The migration MUST preserve each object's uid.

The migration MUST preserve each object's creationTimestamp.

Writers MUST be frozen from the start of the export until the APIService is available.

The APIService MUST NOT become available before the import has completed.

The APIService MUST NOT become available while any imported object that other objects reference by ownerReference is missing.

The initial index sequence for each resource type MUST be greater than the source etcd revision at export time.

A continuous export of all fleet objects MUST run from the first day of phase one.

The cutover MUST be rehearsed on a copy of the management cluster before it runs on the real one.

## 8. Security

Consensus traffic between members MUST use mutual TLS.

The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.

The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.

## 9. Operations

Every voter MUST persist its acceptor state to a durable volume.

A rolling upgrade MUST keep a majority of voters available at all times.

A majority-loss recovery procedure MUST be documented.

The majority-loss recovery procedure MUST be rehearsed before phase two.

Cask MUST expose a health signal for quorum state.

Cask MUST expose counters for lease expiries and for rejected fence regressions.

## 10. Verification

Every safety rule in sections 3 and 5 MUST be an invariant in the Quint model.

The Quint model MUST include a negative control for each invariant that fails when the rule is omitted.

Every requirement in this document MUST have a Duvet citation or a Duvet exception with a reason.

Every MUST in sections 3, 4, and 5 MUST have a Duvet test citation.

Traces generated from the Quint model MUST be replayed against the extension server in a Go test.

End-to-end tests MUST use sigs.k8s.io/e2e-framework against kind clusters.

End-to-end tests MUST NOT use Ginkgo.

End-to-end tests MUST NOT use envtest.

CI MUST run the unit tests with the race detector.

CI MUST run the Quint checks, the Duvet report, the simulator gate, and the end-to-end suite.

A change that lowers Duvet coverage MUST fail CI.
