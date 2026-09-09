// Package kvstore is a tiny replicated key-value state machine sitting on
// top of raft.Raft: every Get/Put/Delete is proposed as a log entry, and is
// only applied to the in-memory map once Raft reports it committed - the
// standard "replicated state machine" pattern the whole Raft paper exists
// to support.
package kvstore

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Amarjeet06/raft-kv-store/raft"
)

var ErrNotLeader = errors.New("not leader")
var ErrTimeout = errors.New("timed out waiting for commit")

type opKind int

const (
	opPut opKind = iota
	opDelete
)

// Op is what actually gets replicated through the Raft log. Command is
// stored as an interface{} inside raft.LogEntry, so this is the concrete
// type it holds for this application.
type Op struct {
	Kind  opKind
	Key   string
	Value string
	// ClientSeq lets the state machine de-duplicate a command that gets
	// proposed twice (e.g. a client retries after a leader change before
	// learning its first attempt actually committed) - without this, a
	// retried Put could double-apply in a way that matters for
	// non-idempotent operations, and it's the same "exactly-once despite
	// at-least-once delivery" problem every replicated log faces.
	ClientID  int64
	ClientSeq int64
}

type pendingWaiter struct {
	term int
	ch   chan Op
}

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
	rf   *raft.Raft

	pending    map[int]pendingWaiter // log index -> waiter expecting that index to be applied
	lastSeq    map[int64]int64       // last applied ClientSeq per ClientID, for de-duplication
	lastResult map[int64]string      // cached result of the last applied op per client, for a de-duped Get-after-Put read
}

func New(rf *raft.Raft, applyCh chan raft.ApplyMsg) *KVStore {
	kv := &KVStore{
		rf:         rf,
		data:       make(map[string]string),
		pending:    make(map[int]pendingWaiter),
		lastSeq:    make(map[int64]int64),
		lastResult: make(map[int64]string),
	}
	go kv.applyLoop(applyCh)
	return kv
}

func (kv *KVStore) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}
		// The Raft layer treats Command as an opaque interface{} and, over
		// the real HTTPTransport, round-trips every log entry through
		// JSON to cross the wire between processes - which decodes any
		// interface{} holding a struct back as a plain map[string]any,
		// not the original Op (Go's encoding/json has no way to recover a
		// concrete type from an interface{} target). Encoding Op as a
		// JSON string before it ever reaches Start() sidesteps that: a
		// JSON string decodes back into a Go string in both the
		// in-process simulated Network (never serialized at all) and the
		// real HTTP path (actually serialized), so this same decode step
		// is correct either way instead of needing a transport-specific
		// special case.
		raw, ok := msg.Command.(string)
		if !ok {
			continue
		}
		var op Op
		if err := json.Unmarshal([]byte(raw), &op); err != nil {
			continue
		}

		kv.mu.Lock()
		if kv.lastSeq[op.ClientID] < op.ClientSeq {
			switch op.Kind {
			case opPut:
				kv.data[op.Key] = op.Value
			case opDelete:
				delete(kv.data, op.Key)
			}
			kv.lastSeq[op.ClientID] = op.ClientSeq
		}
		w, ok := kv.pending[msg.CommandIndex]
		delete(kv.pending, msg.CommandIndex)
		kv.mu.Unlock()

		if ok {
			w.ch <- op
		}
	}
}

// propose submits op through Raft and blocks until it's actually committed
// at that index (verifying the entry AT that index really is ours, since a
// leader change can overwrite an uncommitted index with someone else's
// entry - the classic case Start()'s returned index alone doesn't protect
// against).
func (kv *KVStore) propose(op Op) error {
	payload, err := json.Marshal(op)
	if err != nil {
		return err
	}
	index, term, isLeader := kv.rf.Start(string(payload))
	if !isLeader {
		return ErrNotLeader
	}

	ch := make(chan Op, 1)
	kv.mu.Lock()
	kv.pending[index] = pendingWaiter{term: term, ch: ch}
	kv.mu.Unlock()

	select {
	case committed := <-ch:
		if committed.ClientID != op.ClientID || committed.ClientSeq != op.ClientSeq {
			return ErrNotLeader // our proposal was superseded by a different leader's entry at this index
		}
		return nil
	case <-time.After(2 * time.Second):
		kv.mu.Lock()
		delete(kv.pending, index)
		kv.mu.Unlock()
		return ErrTimeout
	}
}

func (kv *KVStore) Put(key, value string, clientID, seq int64) error {
	return kv.propose(Op{Kind: opPut, Key: key, Value: value, ClientID: clientID, ClientSeq: seq})
}

func (kv *KVStore) Delete(key string, clientID, seq int64) error {
	return kv.propose(Op{Kind: opDelete, Key: key, ClientID: clientID, ClientSeq: seq})
}

// Get is served from local state without going through Raft. That's a
// deliberate, documented consistency trade-off: it's fast and simple, but
// on a stale/partitioned-away leader it can return an old value instead of
// erroring - true linearizable reads need a ReadIndex or lease mechanism,
// which is out of scope here (see README's "Known Limitations").
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	out := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		out[k] = v
	}
	return out
}
