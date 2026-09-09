package raft

import "sync"

// PersistentState is exactly the subset of Raft state that must survive a
// real crash-and-restart (§5.1): currentTerm, votedFor, and the log.
// Losing any of these can cause a restarted node to vote twice in the same
// term or re-elect over already-committed entries.
// PersistenceStore is anything that can durably save/load Raft's
// persistent state (currentTerm, votedFor, log). Persister below is the
// in-memory stand-in the test suite uses; FilePersister (file_persister.go)
// is the real on-disk implementation cmd/kvserver uses.
type PersistenceStore interface {
	Save(PersistentState)
	Load() (PersistentState, bool)
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// Persister is a minimal in-memory stand-in for durable disk storage, used
// so the test suite can simulate a node crashing and restarting (Kill a
// Raft, then Make() a fresh one against the SAME Persister) without needing
// an actual filesystem. cmd/kvserver uses a file-backed implementation for
// real deployments (see FilePersister).
type Persister struct {
	mu    sync.Mutex
	saved PersistentState
	valid bool
}

func MakePersister() *Persister { return &Persister{} }

func (p *Persister) Save(s PersistentState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saved = s
	p.valid = true
}

func (p *Persister) Load() (PersistentState, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saved, p.valid
}
