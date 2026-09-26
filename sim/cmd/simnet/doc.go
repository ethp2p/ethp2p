// Package simnet provides the simctl test executable for in-process network
// simulations. The entry point is a Go test because testing/synctest requires
// a *testing.T to run the simulated clock and goroutines.
package simnet
