package raft

import (
	"path/filepath"
	"testing"
)

// TestFilePersisterRoundTrip verifies the on-disk persistence cmd/kvserver
// relies on for real crash recovery: what's Saved is exactly what a fresh
// FilePersister pointed at the same path Loads back, including after the
// original process is gone (a new *FilePersister value, same file).
func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state.json")

	p1 := NewFilePersister(path)
	if _, ok := p1.Load(); ok {
		t.Fatalf("Load on a nonexistent file should report ok=false")
	}

	want := PersistentState{
		CurrentTerm: 7,
		VotedFor:    2,
		Log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: "set x=1"},
			{Term: 3, Command: "set y=2"},
			{Term: 7, Command: "set z=3"},
		},
	}
	p1.Save(want)

	// Simulate a real restart: a brand new FilePersister value (as
	// cmd/kvserver constructs on every startup), reading the same path.
	p2 := NewFilePersister(path)
	got, ok := p2.Load()
	if !ok {
		t.Fatalf("Load after Save reported ok=false")
	}
	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor {
		t.Fatalf("got term/votedFor %d/%d, want %d/%d", got.CurrentTerm, got.VotedFor, want.CurrentTerm, want.VotedFor)
	}
	if len(got.Log) != len(want.Log) {
		t.Fatalf("got %d log entries, want %d", len(got.Log), len(want.Log))
	}
	for i := range want.Log {
		if got.Log[i].Term != want.Log[i].Term || got.Log[i].Command != want.Log[i].Command {
			t.Fatalf("log[%d] = %+v, want %+v", i, got.Log[i], want.Log[i])
		}
	}
}

// TestFilePersisterOverwrite verifies a second Save fully replaces the
// first (the temp-file-then-rename write must not leave stale bytes behind)
// - exactly what happens every time a real node persists a new vote/term.
func TestFilePersisterOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	p := NewFilePersister(path)

	p.Save(PersistentState{CurrentTerm: 1, VotedFor: 0, Log: []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 1, Command: "b"}, {Term: 1, Command: "c"}}})
	p.Save(PersistentState{CurrentTerm: 5, VotedFor: -1, Log: []LogEntry{{Term: 0}}})

	got, ok := p.Load()
	if !ok {
		t.Fatalf("Load reported ok=false")
	}
	if got.CurrentTerm != 5 || got.VotedFor != -1 || len(got.Log) != 1 {
		t.Fatalf("second Save did not fully replace first: got %+v", got)
	}
}

// TestFilePersisterImplementsInterface is a compile-time check that
// FilePersister satisfies the same PersistenceStore interface the raft
// package's Make() accepts and the in-memory Persister already implements
// - i.e. cmd/kvserver's real on-disk persistence is a drop-in replacement,
// not a special case.
func TestFilePersisterImplementsInterface(t *testing.T) {
	var _ PersistenceStore = NewFilePersister(filepath.Join(t.TempDir(), "x.json"))
	var _ PersistenceStore = MakePersister()
}
