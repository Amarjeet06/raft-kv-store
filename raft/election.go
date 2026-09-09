package raft

import "sync"

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// HandleRequestVote is the RPC handler invoked (via the simulated Network)
// on the receiving peer. It implements the two safety rules from §5.2/§5.4.1:
// never vote twice in the same term, and never vote for a candidate whose
// log is behind your own.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}

	if args.Term < rf.currentTerm {
		return reply // stale candidate, reject
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionTimer() // granting a vote counts as hearing from a legitimate peer
	}
	return reply
}

func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	return rf.net.SendRequestVote(rf.me, peer, args)
}

// startElection converts this peer to Candidate, votes for itself, and
// solicits votes from every other peer in parallel. If it wins a majority
// while still in the same term it started the election in, it becomes
// leader and immediately asserts authority with a round of heartbeats.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	peers := append([]int(nil), rf.peerIDs...)
	rf.mu.Unlock()

	votes := 1 // vote for self
	var mu sync.Mutex
	majority := len(peers)/2 + 1

	// A cluster of size 1 (or any case where the self-vote alone already
	// clears the majority bar) must win immediately - there is no peer
	// left to send a RequestVote to, so the usual "check majority when a
	// reply arrives" path would otherwise never fire and this node would
	// sit as a Candidate forever.
	if votes >= majority {
		rf.mu.Lock()
		if rf.state == Candidate && rf.currentTerm == term {
			rf.becomeLeaderLocked()
		}
		rf.mu.Unlock()
		return
	}

	for _, p := range peers {
		if p == rf.me {
			continue
		}
		go func(peer int) {
			reply, ok := rf.sendRequestVote(peer, args)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != Candidate || rf.currentTerm != term {
				return // stale reply, election already resolved or superseded
			}
			if reply.VoteGranted {
				mu.Lock()
				votes++
				v := votes
				mu.Unlock()
				if v >= majority {
					rf.becomeLeaderLocked()
				}
			}
		}(p)
	}
}

// becomeLeaderLocked transitions to Leader. Caller must hold rf.mu.
func (rf *Raft) becomeLeaderLocked() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for _, p := range rf.peerIDs {
		rf.nextIndex[p] = rf.lastLogIndex() + 1
		rf.matchIndex[p] = 0
	}
	go rf.broadcastAppendEntries()
}
