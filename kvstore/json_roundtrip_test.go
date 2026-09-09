package kvstore

import (
	"encoding/json"
	"testing"
)

// TestOpSurvivesRealTransportJSONRoundTrip guards against the exact bug
// found while smoke-testing the real HTTPTransport: raft.LogEntry.Command
// is a bare interface{}, and raft/http_transport.go JSON-encodes every
// AppendEntriesArgs (including its Entries) to actually cross the wire
// between processes. Go's encoding/json has no way to decode an
// interface{} field back into its original concrete struct type, so an Op
// value placed directly in Command arrives on the other side as a
// map[string]interface{} and panics applyLoop's type assertion (this
// never showed up against the in-process simulated Network, since nothing
// there is ever actually serialized). propose() works around this by
// JSON-encoding Op to a string before calling Start() - this test proves
// that string, and only that string, survives exactly the
// marshal/unmarshal-into-interface{} round trip a real AppendEntries RPC
// performs.
func TestOpSurvivesRealTransportJSONRoundTrip(t *testing.T) {
	op := Op{Kind: opPut, Key: "k", Value: "v", ClientID: 7, ClientSeq: 3}

	payload, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	command := string(payload) // exactly what propose() now hands to raft.Start()

	// Simulate what HTTPTransport does: marshal a struct carrying this
	// command as an interface{} field (as AppendEntriesArgs.Entries[i]
	// .Command does), send it over the wire (here: just JSON round-trip
	// it), and decode into that same interface{}-typed field.
	type wireEntry struct {
		Command interface{} `json:"command"`
	}
	wireBytes, err := json.Marshal(wireEntry{Command: command})
	if err != nil {
		t.Fatalf("marshal wire entry: %v", err)
	}
	var received wireEntry
	if err := json.Unmarshal(wireBytes, &received); err != nil {
		t.Fatalf("unmarshal wire entry: %v", err)
	}

	raw, ok := received.Command.(string)
	if !ok {
		t.Fatalf("Command decoded as %T, not string - this is exactly the bug applyLoop guards against", received.Command)
	}
	var got Op
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal Op from wire string: %v", err)
	}
	if got != op {
		t.Fatalf("Op did not survive the round trip: got %+v, want %+v", got, op)
	}
}

// TestOpDirectlyInInterfaceBreaksAcrossJSON documents, and locks in, WHY
// the fix above is necessary: placing an Op directly in an interface{}
// field and JSON round-tripping it does NOT come back as an Op.
func TestOpDirectlyInInterfaceBreaksAcrossJSON(t *testing.T) {
	op := Op{Kind: opPut, Key: "k", Value: "v", ClientID: 7, ClientSeq: 3}
	type wireEntry struct {
		Command interface{} `json:"command"`
	}
	wireBytes, err := json.Marshal(wireEntry{Command: op})
	if err != nil {
		t.Fatalf("marshal wire entry: %v", err)
	}
	var received wireEntry
	if err := json.Unmarshal(wireBytes, &received); err != nil {
		t.Fatalf("unmarshal wire entry: %v", err)
	}
	if _, ok := received.Command.(Op); ok {
		t.Fatalf("expected an Op placed directly in interface{} to NOT survive a JSON round trip (it decoded as %T) - if this starts passing, encoding/json's behavior changed and the string-encoding workaround in propose()/applyLoop is no longer needed", received.Command)
	}
	if _, ok := received.Command.(map[string]interface{}); !ok {
		t.Fatalf("expected the broken round trip to decode as map[string]interface{}, got %T", received.Command)
	}
}
