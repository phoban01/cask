//go:build race

package nebula_test

// raceEnabled reports whether the build has the race detector on. Nebula's own
// handshake manager and lighthouse worker race against each other under -race
// (the data race is wholly inside github.com/slackhq/nebula, see
// handshake_manager.go / lighthouse.go), so the live-tunnel test is skipped in
// race builds and runs in the ordinary `go test ./...` path instead.
const raceEnabled = true
