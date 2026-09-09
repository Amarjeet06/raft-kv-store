package raft

import "sort"

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// ConflictIndex/ConflictTerm let a follower tell the leader exactly
	// where its log diverges, so the leader can jump nextIndex back by
	// more than one entry per RPC instead of a slow linear backoff
	// (the paper's optional but important §5.3 optimization).
	ConflictIndex int
	ConflictTerm  int
}

// HandleAppendEntries is the RPC handler for both heartbeats (Entries==nil)
// and real log replication. It enforces the log-matching property: it only
// accepts new entries if it can verify, via PrevLogIndex/PrevLogTerm, that
// its own log agrees with the leader's up to that point.
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &AppendEntriesReply{Term: rf.currentTerm, Success: false}

	if args.Term < rf.currentTerm {
		return reply // stale leader
	}
	if args.Term > rf.currentTerm || rf.state != Follower {
		rf.becomeFollower(args.Term)
	} else {
		rf.resetElectionTimer() // valid heartbeat/append from the current leader
	}
	reply.Term = rf.currentTerm

	// Consistency check: do we even have PrevLogIndex, and does its term match?
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return reply
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx-1].Term == conflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		reply.ConflictTerm = conflictTerm
		return reply
	}

	// Log matches through PrevLogIndex. Splice in the leader's entries,
	// truncating our own log at the first point of disagreement (§5.3).
	insertAt := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertAt + i
		if idx <= rf.lastLogIndex() {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			continue
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if rf.lastLogIndex() < newCommit {
			newCommit = rf.lastLogIndex()
		}
		rf.commitIndex = newCommit
	}

	reply.Success = true
	return reply
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	return rf.net.SendAppendEntries(rf.me, peer, args)
}

// broadcastAppendEntries sends every peer either a heartbeat or, if the
// leader has entries that peer hasn't matched yet, the actual entries it's
// missing. It's called on every heartbeat tick and also right after Start()
// so new commands propagate promptly instead of waiting for the next tick.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	peers := append([]int(nil), rf.peerIDs...)
	rf.mu.Unlock()

	for _, p := range peers {
		if p == rf.me {
			continue
		}
		go rf.replicateTo(p, term)
	}
}

func (rf *Raft) replicateTo(peer int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevLogIndex := next - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := append([]LogEntry(nil), rf.log[next:]...)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply, ok := rf.sendAppendEntries(peer, args)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return // stale response from an election/term we've moved past
	}

	if reply.Success {
		newMatch := prevLogIndex + len(entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
			rf.nextIndex[peer] = newMatch + 1
		}
		rf.tryAdvanceCommitIndexLocked()
		return
	}

	// Fast log backtrack using the follower's conflict hint.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
		return
	}
	lastIdxOfConflictTerm := -1
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			lastIdxOfConflictTerm = i
			break
		}
	}
	if lastIdxOfConflictTerm != -1 {
		rf.nextIndex[peer] = lastIdxOfConflictTerm + 1
	} else {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
}

// tryAdvanceCommitIndexLocked implements §5.4.2: a leader may only conclude
// an entry is committed by counting replicas for an entry from its OWN
// current term. Committing an older-term entry just because a majority
// happens to have it can, in rare interleavings, later be overwritten by a
// future leader — this rule is what rules that out. Caller holds rf.mu.
func (rf *Raft) tryAdvanceCommitIndexLocked() {
	if rf.state != Leader {
		return
	}
	matches := make([]int, 0, len(rf.peerIDs))
	for _, p := range rf.peerIDs {
		if p == rf.me {
			matches = append(matches, rf.lastLogIndex())
		} else {
			matches = append(matches, rf.matchIndex[p])
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))
	majorityMatch := matches[len(matches)/2]

	if majorityMatch > rf.commitIndex && rf.log[majorityMatch].Term == rf.currentTerm {
		rf.commitIndex = majorityMatch
	}
}

// Start submits a new command for replication. It returns immediately: the
// index the command WILL occupy if this peer stays leader long enough to
// commit it, the current term, and whether this peer is currently the
// leader at all. The caller (the KV layer) is responsible for watching
// applyCh to learn when/if the command actually commits.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index = rf.lastLogIndex()
	term = rf.currentTerm
	// With zero other peers (or if replies already caught us up), nothing
	// will ever call the leader's own commit-advancement check on our
	// behalf - a single-node cluster's leader IS a majority of one, so it
	// must be able to commit off its own log alone without waiting on any
	// reply that will never come.
	rf.tryAdvanceCommitIndexLocked()
	go rf.broadcastAppendEntries()
	return index, term, true
}
