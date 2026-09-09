package raft

import "time"

// applier watches for newly-committed entries and pushes them to applyCh in
// strict index order, exactly once each — the guarantee every replicated
// state machine (our KV store included) depends on.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		var toApply []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			toApply = append(toApply, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, msg := range toApply {
			rf.applyCh <- msg
		}
		if len(toApply) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
