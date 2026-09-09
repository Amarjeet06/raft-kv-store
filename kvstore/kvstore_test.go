package kvstore

import (
	"testing"
	"time"

	"github.com/Amarjeet06/raft-kv-store/raft"
)

type kvCluster struct {
	t     *testing.T
	net   *raft.Network
	rafts []*raft.Raft
	kvs   []*KVStore
	n     int
}

func makeKVCluster(t *testing.T, n int) *kvCluster {
	net := raft.MakeNetwork(n)
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	kc := &kvCluster{t: t, net: net, n: n}
	for i := 0; i < n; i++ {
		ch := make(chan raft.ApplyMsg, 100)
		rf := raft.Make(net, i, peers, ch, raft.MakePersister())
		net.RegisterServer(i, rf)
		kv := New(rf, ch)
		kc.rafts = append(kc.rafts, rf)
		kc.kvs = append(kc.kvs, kv)
	}
	return kc
}

func (kc *kvCluster) shutdown() {
	for _, rf := range kc.rafts {
		rf.Kill()
	}
}

// leader retries Put/Delete against whichever node is currently willing to
// accept it, exactly as a real client with no leader hint would.
func (kc *kvCluster) put(key, value string, clientID, seq int64) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, kv := range kc.kvs {
			if err := kv.Put(key, value, clientID, seq); err == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	kc.t.Fatalf("put(%q,%q) never succeeded against any peer", key, value)
}

func TestPutGetSingleNode(t *testing.T) {
	kc := makeKVCluster(t, 1)
	defer kc.shutdown()
	kc.put("a", "1", 1, 1)
	v, ok := kc.kvs[0].Get("a")
	if !ok || v != "1" {
		t.Fatalf("Get(a) = (%q,%v), want (1,true)", v, ok)
	}
}

func TestPutReplicatesToAllPeers(t *testing.T) {
	kc := makeKVCluster(t, 3)
	defer kc.shutdown()
	kc.put("x", "42", 1, 1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, kv := range kc.kvs {
			v, ok := kv.Get("x")
			if !ok || v != "42" {
				allMatch = false
			}
		}
		if allMatch {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not all peers converged on x=42 within deadline")
}

func TestWriteSurvivesLeaderFailover(t *testing.T) {
	kc := makeKVCluster(t, 5)
	defer kc.shutdown()

	kc.put("before", "1", 1, 1)

	// Find and crash the current leader mid-operation.
	var leaderIdx = -1
	for i, rf := range kc.rafts {
		if _, isLeader := rf.GetState(); isLeader {
			leaderIdx = i
		}
	}
	if leaderIdx == -1 {
		t.Fatalf("no leader found")
	}
	kc.rafts[leaderIdx].Kill()
	kc.net.SetNodeUp(leaderIdx, false)

	// A write after the crash must still succeed against the new leader.
	kc.put("after", "2", 1, 2)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok1, ok2 := false, false
		for i, kv := range kc.kvs {
			if i == leaderIdx {
				continue
			}
			if v, ok := kv.Get("before"); ok && v == "1" {
				ok1 = true
			}
			if v, ok := kv.Get("after"); ok && v == "2" {
				ok2 = true
			}
		}
		if ok1 && ok2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("writes before/after the leader crash did not both converge on the surviving peers")
}

func TestDuplicateProposalIsNotDoubleApplied(t *testing.T) {
	kc := makeKVCluster(t, 3)
	defer kc.shutdown()

	kc.put("counter", "v1", 7, 1)
	// Same ClientID+ClientSeq submitted again (simulating a client retry
	// after an ambiguous timeout) must not re-apply as if it were a new
	// operation - the state machine should recognize it as already done.
	for _, kv := range kc.kvs {
		_ = kv.Put("counter", "v2-should-be-ignored", 7, 1)
	}
	time.Sleep(200 * time.Millisecond)

	v, ok := kc.kvs[0].Get("counter")
	if !ok || v != "v1" {
		t.Fatalf("duplicate ClientSeq was re-applied: Get(counter) = (%q,%v), want (v1,true)", v, ok)
	}
}
