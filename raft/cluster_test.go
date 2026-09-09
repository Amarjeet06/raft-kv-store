package raft

import (
	"fmt"
	"testing"
	"time"
)

// testCluster wires up n Raft peers against a shared simulated Network,
// giving the actual test functions a small, readable API for driving and
// inspecting the cluster. It keeps each peer's Persister around so a test
// can simulate a real crash-and-restart (Kill the old instance, Make() a
// fresh one against the same Persister) rather than merely cutting network
// reachability, which models isolation, not a crash - an important
// distinction Raft itself treats differently.
type testCluster struct {
	t          *testing.T
	net        *Network
	rafts      []*Raft
	applyCh    []chan ApplyMsg
	persisters []*Persister
	peers      []int
	n          int
	applied    []map[int]bool // per-peer set of indices already observed via applyCh, so repeated checks don't re-drain the channel
}

func makeCluster(t *testing.T, n int) *testCluster {
	net := MakeNetwork(n)
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	tc := &testCluster{t: t, net: net, n: n, peers: peers}
	for i := 0; i < n; i++ {
		ch := make(chan ApplyMsg, 100)
		p := MakePersister()
		rf := Make(net, i, peers, ch, p)
		net.RegisterServer(i, rf)
		tc.rafts = append(tc.rafts, rf)
		tc.applyCh = append(tc.applyCh, ch)
		tc.persisters = append(tc.persisters, p)
		tc.applied = append(tc.applied, make(map[int]bool))
	}
	return tc
}

func (tc *testCluster) shutdown() {
	for _, rf := range tc.rafts {
		rf.Kill()
	}
}

// crash simulates peer i's process dying: its goroutines stop entirely (not
// just "network unreachable"), so it can neither send nor receive anything,
// including admin/status calls - a partitioned-but-alive node would still
// answer GetState() locally, a genuinely crashed one would not.
func (tc *testCluster) crash(i int) {
	tc.rafts[i].Kill()
	tc.net.SetNodeUp(i, false)
}

// restart simulates peer i's process restarting and reading its persisted
// state back off disk, exactly as TestPersistenceAcrossRestart's manual
// version does, just reusable.
func (tc *testCluster) restart(i int) {
	tc.net.SetNodeUp(i, true)
	ch := make(chan ApplyMsg, 100)
	rf := Make(tc.net, i, tc.peers, ch, tc.persisters[i])
	tc.net.RegisterServer(i, rf)
	tc.rafts[i] = rf
	tc.applyCh[i] = ch
}

// checkOneLeader polls until exactly one LIVE peer believes it is leader
// for the latest term observed among live peers, then returns that
// leader's index. A crashed peer is excluded, since a real caller could
// never reach it to ask.
func (tc *testCluster) checkOneLeader() int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		leadersByTerm := make(map[int][]int)
		for i, rf := range tc.rafts {
			if !tc.net.IsUp(i) {
				continue
			}
			term, isLeader := rf.GetState()
			if isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], i)
			}
		}
		lastTerm := -1
		for term := range leadersByTerm {
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm != -1 && len(leadersByTerm[lastTerm]) == 1 {
			return leadersByTerm[lastTerm][0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("no single stable leader emerged among live peers")
	return -1
}

// submit sends command through whichever LIVE peer is currently leader,
// retrying until one accepts it (mirrors how a real client with no leader
// hint behaves).
// waitForLeaderIn polls until some peer in `group` believes it is leader,
// tolerating the split-vote retries the Raft paper describes as a normal
// (if occasionally slow) part of randomized-timeout election - a fixed
// short sleep is not long enough to be robust against an unlucky split
// vote needing a second or third round to resolve.
func (tc *testCluster) waitForLeaderIn(group []int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, i := range group {
			if _, isLeader := tc.rafts[i].GetState(); isLeader {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

func (tc *testCluster) submit(command interface{}) (index int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for i, rf := range tc.rafts {
			if !tc.net.IsUp(i) {
				continue
			}
			if idx, _, isLeader := rf.Start(command); isLeader {
				return idx
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("no live leader accepted command %v", command)
	return -1
}

// waitCommitted blocks until at least `atLeast` peers have applied the
// entry at `index` with the expected command (checked against tc.applied so
// a peer already confirmed by an earlier call isn't required to re-emit it
// on the channel), or fails after the timeout.
func (tc *testCluster) waitCommitted(index int, expected interface{}, atLeast int) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, ch := range tc.applyCh {
		drain:
			for {
				select {
				case msg := <-ch:
					if msg.CommandValid {
						if msg.CommandIndex == index && fmt.Sprint(msg.Command) != fmt.Sprint(expected) {
							tc.t.Fatalf("peer %d applied wrong command at index %d: got %v want %v", i, index, msg.Command, expected)
						}
						tc.applied[i][msg.CommandIndex] = true
					}
				default:
					break drain
				}
			}
		}
		count := 0
		for i := range tc.rafts {
			if tc.applied[i][index] {
				count++
			}
		}
		if count >= atLeast {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	count := 0
	for i := range tc.rafts {
		if tc.applied[i][index] {
			count++
		}
	}
	tc.t.Fatalf("only %d/%d peers committed index %d within deadline (want >= %d)", count, tc.n, index, atLeast)
}
