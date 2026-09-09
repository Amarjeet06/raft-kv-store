// Command kvserver runs one node of a real, multi-process Raft-replicated
// key-value cluster: it wires the raft package's HTTPTransport (actual
// sockets between processes, not the in-process simulated Network the test
// suite uses) and a FilePersister (actual on-disk state, so the node
// survives a real crash-and-restart) up to the kvstore replicated state
// machine, and serves a small JSON HTTP API for Get/Put/Delete.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Amarjeet06/raft-kv-store/kvstore"
	"github.com/Amarjeet06/raft-kv-store/raft"
)

// parsePeers parses "0=127.0.0.1:8000,1=127.0.0.1:8001,2=127.0.0.1:8002"
// into an id -> "host:port" map, and returns the sorted list of ids.
func parsePeers(spec string) (map[int]string, []int) {
	addrs := make(map[int]string)
	var ids []int
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			log.Fatalf("bad -peers entry %q, want id=host:port", part)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			log.Fatalf("bad peer id %q: %v", kv[0], err)
		}
		addrs[id] = kv[1]
		ids = append(ids, id)
	}
	return addrs, ids
}

type putReq struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID int64  `json:"client_id"`
	Seq      int64  `json:"seq"`
}

type deleteReq struct {
	Key      string `json:"key"`
	ClientID int64  `json:"client_id"`
	Seq      int64  `json:"seq"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func main() {
	id := flag.Int("id", -1, "this node's numeric id (must be a key in -peers)")
	peersSpec := flag.String("peers", "", "comma-separated id=host:port list for every node in the cluster, including this one")
	dataDir := flag.String("datadir", "", "directory to persist Raft state in (created if missing)")
	listen := flag.String("listen", "", "address to listen on (defaults to this node's own entry in -peers)")
	flag.Parse()

	if *id < 0 || *peersSpec == "" {
		log.Fatal("both -id and -peers are required")
	}
	addrs, ids := parsePeers(*peersSpec)
	if _, ok := addrs[*id]; !ok {
		log.Fatalf("-id %d is not present in -peers", *id)
	}
	listenAddr := *listen
	if listenAddr == "" {
		listenAddr = addrs[*id]
	}
	dir := *dataDir
	if dir == "" {
		dir = filepath.Join(".", "data", strconv.Itoa(*id))
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("mkdir %s: %v", dir, err)
	}

	transport := raft.NewHTTPTransport(addrs)
	persister := raft.NewFilePersister(filepath.Join(dir, "raft-state.json"))
	applyCh := make(chan raft.ApplyMsg, 64)
	rf := raft.Make(transport, *id, ids, applyCh, persister)
	kv := kvstore.New(rf, applyCh)

	mux := http.NewServeMux()
	mux.Handle("/raft/", raft.Handler(rf))

	mux.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value, found := kv.Get(key)
		writeJSON(w, http.StatusOK, map[string]interface{}{"value": value, "found": found})
	})

	mux.HandleFunc("/kv/put", func(w http.ResponseWriter, r *http.Request) {
		var req putReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := kv.Put(req.Key, req.Value, req.ClientID, req.Seq); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	mux.HandleFunc("/kv/delete", func(w http.ResponseWriter, r *http.Request) {
		var req deleteReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := kv.Delete(req.Key, req.ClientID, req.Seq); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		term, isLeader := rf.GetState()
		writeJSON(w, http.StatusOK, map[string]interface{}{"id": *id, "term": term, "leader": isLeader})
	})

	log.Printf("kvserver node %d listening on %s (peers: %v)", *id, listenAddr, addrs)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
