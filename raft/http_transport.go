package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTPTransport is the real-network Transport implementation used by
// cmd/kvserver: each peer runs an HTTP server (see Handler, below)
// exposing /raft/request_vote and /raft/append_entries, and
// SendRequestVote/SendAppendEntries POST JSON-encoded RPC args to the
// target peer's address and decode its JSON reply - the same RPCs the
// simulated Network delivers in-process during tests, now actually
// crossing a socket between independent processes.
type HTTPTransport struct {
	Addrs  map[int]string // peer id -> "host:port"
	Client *http.Client
}

func NewHTTPTransport(addrs map[int]string) *HTTPTransport {
	return &HTTPTransport{
		Addrs:  addrs,
		Client: &http.Client{Timeout: 300 * time.Millisecond},
	}
}

func (t *HTTPTransport) post(to int, path string, body, out interface{}) bool {
	addr, ok := t.Addrs[to]
	if !ok {
		return false
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return false
	}
	resp, err := t.Client.Post(fmt.Sprintf("http://%s%s", addr, path), "application/json", bytes.NewReader(buf))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return json.NewDecoder(resp.Body).Decode(out) == nil
}

func (t *HTTPTransport) SendRequestVote(from, to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	ok := t.post(to, "/raft/request_vote", args, reply)
	return reply, ok
}

func (t *HTTPTransport) SendAppendEntries(from, to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	ok := t.post(to, "/raft/append_entries", args, reply)
	return reply, ok
}

// Handler returns an http.Handler exposing this peer's RPC endpoints so
// every other peer's HTTPTransport can reach it.
func Handler(rf *Raft) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/request_vote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := rf.HandleRequestVote(&args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})
	mux.HandleFunc("/raft/append_entries", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := rf.HandleAppendEntries(&args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})
	return mux
}
