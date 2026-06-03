//go:build !race

package nebula_test

// raceEnabled is false in non-race builds; see raceflag_race.go.
const raceEnabled = false
