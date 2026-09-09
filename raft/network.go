package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Network is a simulated, unreliable network connecting a fixed set of Raft
// peers by integer index. It is used by the test suite to inject the kinds
// of failures a real deployment can hit — dropped nodes, network partitions,
// message reordering, and one-way latency — without needing real sockets.
// This is the same style of harness used to correctness-test production
// Raft implementations: correctness is proven against simulated faults,
// not just against the happy path.
type Network struct {
	mu        sync.Mutex
	reachable map[[2]int]bool // reachable[{from,to}] - can `from` successfully deliver an RPC to `to`?
	nodeUp    map[int]bool    // is node i powered on?
	long      bool            // simulate long, variable network delay
	servers   map[int]*Raft   // id -> the peer that owns it, for routing simulated RPCs
}

func MakeNetwork(n int) *Network {
	net := &Network{
		reachable: make(map[[2]int]bool),
		nodeUp:    make(map[int]bool),
	}
	for i := 0; i < n; i++ {
		net.nodeUp[i] = true
		for j := 0; j < n; j++ {
			net.reachable[[2]int{i, j}] = true
		}
	}
	return net
}

// SetLongDelays enables/disables extra simulated one-way latency, useful for
// shaking out timing-sensitive election bugs.
func (net *Network) SetLongDelays(v bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.long = v
}

// SetReachable controls whether RPCs from `from` to `to` are delivered.
// Used to simulate asymmetric link failures and full partitions.
func (net *Network) SetReachable(from, to int, ok bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.reachable[[2]int{from, to}] = ok
}

// Partition splits the cluster into disjoint groups; nodes can reach others
// in the same group but not nodes in a different group. This is the primary
// tool the safety tests use to prove the "no commit without a majority"
// property.
func (net *Network) Partition(groups [][]int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	groupOf := make(map[int]int)
	for gi, g := range groups {
		for _, id := range g {
			groupOf[id] = gi
		}
	}
	for from := range net.nodeUp {
		for to := range net.nodeUp {
			net.reachable[[2]int{from, to}] = groupOf[from] == groupOf[to]
		}
	}
}

// Heal restores full connectivity between all nodes.
func (net *Network) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	for from := range net.nodeUp {
		for to := range net.nodeUp {
			net.reachable[[2]int{from, to}] = true
		}
	}
}

// SetNodeUp powers a node on/off. A powered-off node neither sends nor
// receives RPCs, simulating a crash (not a graceful shutdown).
func (net *Network) SetNodeUp(id int, up bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodeUp[id] = up
}

func (net *Network) IsUp(id int) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.nodeUp[id]
}

func (net *Network) canDeliver(from, to int) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	if !net.nodeUp[from] || !net.nodeUp[to] {
		return false
	}
	return net.reachable[[2]int{from, to}]
}

func (net *Network) delay() {
	net.mu.Lock()
	long := net.long
	net.mu.Unlock()
	ms := 1 + rand.Intn(4)
	if long {
		ms += rand.Intn(50)
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// Call delivers an RPC from `from` to the given endpoint's handler if the
// simulated network currently allows it, otherwise it blocks briefly (as a
// real timed-out RPC would) and reports failure. `handler` is the callee's
// actual RPC method, invoked directly in-process.
func (net *Network) Call(from, to int, handler func() bool) bool {
	if !net.canDeliver(from, to) {
		net.delay()
		return false
	}
	net.delay()
	if !net.canDeliver(from, to) {
		return false
	}
	return handler()
}

// SendRequestVote and SendAppendEntries implement the Transport interface
// on top of the simulated network above: look up the addressee's *Raft by
// id and invoke its handler directly, in-process, subject to the same
// delay/drop simulation every other RPC goes through.

func (net *Network) SendRequestVote(from, to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	target := net.GetServer(to)
	ok := net.Call(from, to, func() bool {
		if target == nil {
			return false
		}
		*reply = *target.HandleRequestVote(args)
		return true
	})
	return reply, ok
}

func (net *Network) SendAppendEntries(from, to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	target := net.GetServer(to)
	ok := net.Call(from, to, func() bool {
		if target == nil {
			return false
		}
		*reply = *target.HandleAppendEntries(args)
		return true
	})
	return reply, ok
}

// --- server registry, so peers can route simulated RPCs to each other ---

func (net *Network) RegisterServer(id int, rf *Raft) {
	net.mu.Lock()
	defer net.mu.Unlock()
	if net.servers == nil {
		net.servers = make(map[int]*Raft)
	}
	net.servers[id] = rf
}

func (net *Network) GetServer(id int) *Raft {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.servers[id]
}
