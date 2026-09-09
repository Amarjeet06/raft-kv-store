// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm"): leader election via
// randomized timeouts, log replication with the AppendEntries consistency
// check, and the safety rules that prevent a minority partition from
// committing conflicting entries (the election-restriction "up-to-date log"
// check on RequestVote, and the §5.4.2 rule that a leader only commits
// entries from its own current term by counting replicas).
package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "?"
	}
}

// LogEntry is one slot in the replicated log. Index 0 is a dummy sentinel so
// real entries start at index 1, matching the paper's 1-indexed log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg is delivered to the owning application once a log entry has been
// committed by a majority of the cluster and is safe to execute against the
// state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

type Raft struct {
	mu      sync.Mutex
	net     Transport
	me      int
	peerIDs []int
	applyCh chan ApplyMsg
	dead    int32

	// --- persistent state (§5.1, must survive a real restart; the
	// Persister below stands in for disk in this simulated harness) ---
	currentTerm int
	votedFor    int
	log         []LogEntry
	persister   PersistenceStore

	// --- volatile state ---
	state           State
	commitIndex     int
	lastApplied     int
	electionResetAt time.Time
	electionTimeout time.Duration

	// --- leader-only volatile state (reinitialized after each election) ---
	nextIndex  map[int]int
	matchIndex map[int]int
}

func Make(net Transport, me int, peerIDs []int, applyCh chan ApplyMsg, persister PersistenceStore) *Raft {
	rf := &Raft{
		net:         net,
		me:          me,
		peerIDs:     peerIDs,
		applyCh:     applyCh,
		persister:   persister,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // sentinel at index 0
		state:       Follower,
	}
	if persister != nil {
		if saved, ok := persister.Load(); ok {
			rf.currentTerm = saved.CurrentTerm
			rf.votedFor = saved.VotedFor
			rf.log = saved.Log
		}
	}
	rf.resetElectionTimer()
	go rf.ticker()
	go rf.applier()
	return rf
}

func (rf *Raft) Kill()        { atomic.StoreInt32(&rf.dead, 1) }
func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// GetState reports what this peer believes right now: its term and whether
// it currently thinks it's the leader. Used by the KV layer to reject writes
// at non-leaders without a wasted round trip.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// CommitIndex reports this peer's current commit index - useful for
// metrics/monitoring in a real deployment, and for tests asserting that an
// isolated minority leader never advances past a submitted-but-uncommitted
// entry.
func (rf *Raft) commitIndexUnsafeForTest() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastLogTerm() int  { return rf.log[rf.lastLogIndex()].Term }

func (rf *Raft) persist() {
	if rf.persister == nil {
		return
	}
	rf.persister.Save(PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         append([]LogEntry(nil), rf.log...),
	})
}

func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (rf *Raft) resetElectionTimer() {
	rf.electionResetAt = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

// becomeFollower converts to follower state, adopting `term` as current.
// Caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.resetElectionTimer()
}

// ticker drives both election timeouts (as follower/candidate) and the
// leader's heartbeat cadence, checking roughly every 10ms - fine-grained
// enough for fast, reliable tests without busy-looping.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		state := rf.state
		elapsed := time.Since(rf.electionResetAt)
		timeout := rf.electionTimeout
		rf.mu.Unlock()

		if state == Leader {
			go rf.broadcastAppendEntries()
			time.Sleep(heartbeatInterval - 10*time.Millisecond)
			continue
		}
		if elapsed >= timeout {
			go rf.startElection()
		}
	}
}
