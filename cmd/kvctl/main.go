// Command kvctl is a small CLI client for a running raft-kv-store cluster:
// it tries each peer in turn until one accepts the request, since only the
// current Raft leader can serve a write (and this client has no way to
// know in advance which node that is - exactly the "not leader" case
// kvstore.ErrNotLeader exists to signal).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	peersSpec := flag.String("peers", "", "comma-separated host:port list of every node in the cluster")
	flag.Parse()
	args := flag.Args()
	if *peersSpec == "" || len(args) < 2 {
		fatalf("usage: kvctl -peers host1:port1,host2:port2,... <get|put|delete> <key> [value]")
	}

	var addrs []string
	for _, p := range strings.Split(*peersSpec, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			addrs = append(addrs, p)
		}
	}

	cmd, key := args[0], args[1]
	clientID := time.Now().UnixNano()
	seq := int64(1)
	client := &http.Client{Timeout: 500 * time.Millisecond}

	switch cmd {
	case "get":
		// Get is served from each node's local, possibly-stale state (see
		// kvstore.Get's documented non-linearizable trade-off), so the
		// first peer to answer is not necessarily right: a follower that
		// has not yet replicated the latest write will truthfully report
		// "not found" even though the key exists on the leader. Bug found
		// while smoke-testing the real HTTP cluster: an early version of
		// this command returned on the very first response regardless of
		// its Found value, so querying an out-of-date follower produced a
		// false "not found" for a key that had just been written seconds
		// earlier. The fix: only report "not found" after every reachable
		// peer has agreed the key is absent.
		reachedAny := false
		for _, addr := range addrs {
			resp, err := client.Get(fmt.Sprintf("http://%s/kv/get?key=%s", addr, key))
			if err != nil {
				continue
			}
			var out struct {
				Value string `json:"value"`
				Found bool   `json:"found"`
			}
			err = json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if err != nil {
				continue
			}
			reachedAny = true
			if out.Found {
				fmt.Println(out.Value)
				return
			}
		}
		if reachedAny {
			fmt.Println("(not found)")
			return
		}
		fatalf("no reachable peer answered the get")

	case "put":
		if len(args) < 3 {
			fatalf("usage: kvctl -peers ... put <key> <value>")
		}
		value := args[2]
		body, _ := json.Marshal(map[string]interface{}{"key": key, "value": value, "client_id": clientID, "seq": seq})
		for _, addr := range addrs {
			resp, err := client.Post(fmt.Sprintf("http://%s/kv/put", addr), "application/json", bytes.NewReader(body))
			if err != nil {
				continue
			}
			ok := resp.StatusCode == http.StatusOK
			resp.Body.Close()
			if ok {
				fmt.Println("OK")
				return
			}
		}
		fatalf("put failed against every peer (no leader reachable / cluster unavailable)")

	case "delete":
		body, _ := json.Marshal(map[string]interface{}{"key": key, "client_id": clientID, "seq": seq})
		for _, addr := range addrs {
			resp, err := client.Post(fmt.Sprintf("http://%s/kv/delete", addr), "application/json", bytes.NewReader(body))
			if err != nil {
				continue
			}
			ok := resp.StatusCode == http.StatusOK
			resp.Body.Close()
			if ok {
				fmt.Println("OK")
				return
			}
		}
		fatalf("delete failed against every peer (no leader reachable / cluster unavailable)")

	default:
		fatalf("unknown command %q (want get, put, or delete)", cmd)
	}
}
