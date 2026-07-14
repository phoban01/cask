package sim

// Fault is one named adversary the gate can inject at a scheduled step. Inject
// mutates the scenario (the network, the stores, the clock); the gate calls
// Net.Heal between applications to undo network-level faults. Buggify-driven
// faults (whose effect is a buggify.Maybe site firing per profile) implement
// Inject as a no-op and rely on the placed site instead.
//
// The interface lives here, in package sim, so the concrete faults package can
// import sim without a cycle (the arrow is faults → sim, never the reverse).
type Fault interface {
	Name() string
	Inject(s *Sim)
}
