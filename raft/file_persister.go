package raft

import (
	"encoding/json"
	"log"
	"os"
)

// FilePersister persists Raft's currentTerm/votedFor/log to a JSON file on
// disk via write-to-temp-then-rename (so a crash mid-write can never leave
// a half-written, corrupt state file behind). cmd/kvserver uses this
// instead of the in-memory Persister so a real process can restart after a
// crash without violating §5.1's persistence guarantees - a node that
// forgets its vote can vote twice in the same term, and a node that
// forgets committed entries can silently lose data.
type FilePersister struct {
	path string
}

func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path}
}

func (p *FilePersister) Save(s PersistentState) {
	tmp := p.path + ".tmp"
	buf, err := json.Marshal(s)
	if err != nil {
		log.Printf("raft: FilePersister marshal failed: %v", err)
		return
	}
	if err := os.WriteFile(tmp, buf, 0600); err != nil {
		log.Printf("raft: FilePersister write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, p.path); err != nil {
		log.Printf("raft: FilePersister rename failed: %v", err)
	}
}

func (p *FilePersister) Load() (PersistentState, bool) {
	buf, err := os.ReadFile(p.path)
	if err != nil {
		return PersistentState{}, false
	}
	var s PersistentState
	if err := json.Unmarshal(buf, &s); err != nil {
		return PersistentState{}, false
	}
	return s, true
}
