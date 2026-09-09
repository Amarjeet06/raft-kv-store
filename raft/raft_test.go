package raft

import (
	"testing"
	"time"
)

func TestLeaderElection(t *testing.T) {
	tc := makeCluster(t, 5)
	defer tc.shutdown()
	tc.checkOneLeader()
}

// TestReElectionAfterLeaderCrash simulates an actual process crash (not
// just a network blip) and verifies the cluster elects a new leader with a
// higher term, then that the restarted old leader rejoins as a follower
// rather than disrupting the new one.
func TestReElectionAfterLeaderCrash(t *testing.T) {
	tc := makeCluster(t, 5)
	defer tc.shutdown()

	leader1 := tc.checkOneLeader()
	term1, _ := tc.rafts[leader1].GetState()
	tc.crash(leader1)

	leader2 := tc.checkOneLeader()
	if leader2 == leader1 {
		t.Fatalf("crashed leader %d was somehow re-elected while down", leader1)
	}
	term2, _ := tc.rafts[leader2].GetState()
	if term2 <= term1 {
		t.Fatalf("new leader's term %d did not advance past crashed leader's term %d", term2, term1)
	}

	tc.restart(leader1)
	time.Sleep(300 * time.Millisecond)

	stillLeader := tc.checkOneLeader()
	if stillLeader != leader2 {
		t.Fatalf("leadership churned after restarting the old leader: was %d, now %d", leader2, stillLeader)
	}
}

func TestBasicAgreement(t *testing.T) {
	tc := makeCluster(t, 3)
	defer tc.shutdown()
	tc.checkOneLeader()

	for i, cmd := range []string{"set x=1", "set y=2", "set z=3"} {
		idx := tc.submit(cmd)
		if idx != i+1 {
			t.Fatalf("command %d got unexpected log index %d", i, idx)
		}
		tc.waitCommitted(idx, cmd, 3)
	}
}

func TestProgressWithMinorityDown(t *testing.T) {
	tc := makeCluster(t, 5)
	defer tc.shutdown()
	leader := tc.checkOneLeader()

	down := 0
	for i := 0; i < tc.n && down < 2; i++ {
		if i != leader {
			tc.crash(i)
			down++
		}
	}

	idx := tc.submit("still-works")
	tc.waitCommitted(idx, "still-works", 3)
}

// TestNoProgressWithoutMajority is the core split-brain-prevention test.
// It partitions the cluster 2-vs-3 regardless of where the pre-partition
// leader happened to land, then proves the minority side can never commit
// a new entry - even though an isolated former leader may still LOCALLY
// believe it's in charge (a real, well-known Raft property: isolation
// alone doesn't inform a leader it's been superseded, only a majority
// vote does; this is why linearizable reads need a lease/ReadIndex
// mechanism on top of the base algorithm, not covered here).
func TestNoProgressWithoutMajority(t *testing.T) {
	tc := makeCluster(t, 5)
	defer tc.shutdown()
	tc.checkOneLeader()

	minority := []int{0, 1}
	majority := []int{2, 3, 4}
	tc.net.Partition([][]int{minority, majority})

	if tc.waitForLeaderIn(majority, 3*time.Second) == -1 {
		t.Fatalf("no leader emerged in the majority partition %v", majority)
	}

	// Whether or not a minority node still believes itself leader, no
	// command submitted on that side may ever reach a majority - assert
	// directly on the safety property (commit) rather than on the belief.
	committedOnMinority := false
	for _, i := range minority {
		if idx, _, isLeader := tc.rafts[i].Start("minority-write"); isLeader {
			time.Sleep(300 * time.Millisecond)
			if tc.rafts[i].commitIndexUnsafeForTest() >= idx {
				committedOnMinority = true
			}
		}
	}
	if committedOnMinority {
		t.Fatalf("a 2-of-5 minority partition committed an entry - safety violated")
	}

	tc.net.Heal()
}

// TestPartitionHealsAndConverges proves that once a partition heals, every
// peer - including ones that were in the minority and may have a stale
// term - converges on the same committed log.
func TestPartitionHealsAndConverges(t *testing.T) {
	tc := makeCluster(t, 5)
	defer tc.shutdown()
	tc.checkOneLeader()

	majority := []int{2, 3, 4}
	tc.net.Partition([][]int{{0, 1}, majority})

	leader := tc.waitForLeaderIn(majority, 3*time.Second)
	if leader == -1 {
		t.Fatalf("no leader emerged in the majority partition %v", majority)
	}
	idx, _, isLeader := tc.rafts[leader].Start("majority-write")
	if !isLeader {
		t.Fatalf("peer %d stopped being leader between check and Start", leader)
	}

	tc.net.Heal()
	// Single check after healing, for all 5 - checking a subset first
	// would drain their applyCh and make a later full-cluster check
	// under-count peers that already reported in.
	tc.waitCommitted(idx, "majority-write", 5)
}

func TestPersistenceAcrossRestart(t *testing.T) {
	tc := makeCluster(t, 3)
	defer tc.shutdown()

	tc.checkOneLeader()
	idx := tc.submit("durable-write")
	tc.waitCommitted(idx, "durable-write", 3)

	savedLogLen := len(tc.persisters[0].saved.Log)
	if savedLogLen < 2 { // sentinel + the committed entry
		t.Fatalf("persister did not capture the replicated log before crash (len=%d)", savedLogLen)
	}
	savedTerm := tc.persisters[0].saved.CurrentTerm

	tc.crash(0)
	time.Sleep(50 * time.Millisecond)
	tc.restart(0)

	restartedTerm, _ := tc.rafts[0].GetState()
	if restartedTerm < savedTerm {
		t.Fatalf("restarted node forgot its persisted term: had %d, now %d", savedTerm, restartedTerm)
	}

	// The restarted node must catch back up via normal replication.
	tc.waitCommitted(idx, "durable-write", 3)
}
