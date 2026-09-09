# raft-kv-store

A replicated key-value store built from scratch on top of a real implementation of the **Raft consensus algorithm** (Ongaro & Ousterhout, *"In Search of an Understandable Consensus Algorithm"*): randomized-timeout leader election, AppendEntries log replication with the fast conflict-backtrack optimization, and the §5.4.2 safety rule that a leader may only commit entries replicated from its own current term.

**[Live in-browser demo](https://amarjeet06.github.io/raft-kv-store/)** — a genuine port of the same election/replication logic to JavaScript, simulating a 5-node cluster with real network latency, message loss, crashes, and partitions. Kill the leader, partition the network, and watch a new leader get elected and the cluster keep committing writes on the majority side.

## What's actually here

This isn't a toy that only "works" on the happy path. It ships:

- **`raft/`** — the consensus algorithm itself: leader election (`election.go`), log replication (`replication.go`), the core state machine and election/heartbeat timers (`raft.go`), an applier goroutine delivering committed entries in order (`apply.go`), and persistence (`persister.go` — in-memory, used by tests; `file_persister.go` — real on-disk JSON with atomic write-then-rename, used by the deployable server).
- **`kvstore/`** — a replicated key-value state machine on top of `raft.Raft`: every `Get`/`Put`/`Delete` is proposed as a log entry and only applied once Raft reports it committed, with client-request de-duplication via `(ClientID, ClientSeq)` pairs so a retried write can't double-apply.
- **`cmd/kvserver`** — a real, deployable server: wires the algorithm to an actual HTTP transport (`raft/http_transport.go`) and on-disk persistence, so it's a genuine multi-process cluster, not a simulation. `go run ./cmd/kvserver -id 0 -peers "0=host:port,1=host:port,2=host:port"` on three machines (or three terminals) gets you a real Raft cluster.
- **`cmd/kvctl`** — a CLI client (`get`/`put`/`delete`) that retries across peers, since only the current leader can serve a write and the client has no a priori way to know which node that is.
- **`raft/network.go`** — a from-scratch simulated network harness (MIT 6.5840-style): partitions, node crashes, message loss, and randomized one-way latency, all *without real sockets*, so the correctness test suite can inject failures deterministically and fast. Both the simulated `Network` and the real `HTTPTransport` implement the same `Transport` interface — the algorithm code has no idea which one it's talking to.

## Real bugs found via testing

Every project in this portfolio follows the same rule: write the algorithm, then try to break it with a real test suite, not just a happy-path demo. This one turned up two genuine algorithm bugs and three test-methodology bugs.

**1. A single-node cluster could never elect itself leader.** `startElection()` only checked for a winning majority inside the goroutine handling each peer's vote *reply* — but with zero other peers to send `RequestVote` to, that goroutine never runs, so a 1-node "cluster" sat as a `Candidate` forever. Fixed by checking the self-vote against the majority threshold immediately, synchronously, before ever looping over peers.

**2. A single-node leader's writes never committed.** After fixing #1, `kvstore`'s single-node test still failed: `Put` blocked until its own 2-second timeout. Root cause: `tryAdvanceCommitIndexLocked()` — the only place that actually marks a log entry committed — was called exclusively from inside the AppendEntries-reply handler, which `broadcastAppendEntries()` never invokes for `p == rf.me`. With no real peers, that function never ran, so a leader's own appended entries were never marked committed and never reached the state machine. Fixed by calling the commit-check directly inside `Start()` right after appending — a single-node leader *is* a majority of one, and shouldn't need a reply from a peer that will never exist.

**3. The test harness confused "network partitioned" with "process crashed."** `TestReElectionAfterLeaderCrash` failed with a crashed leader supposedly "re-elected while still down" — except it wasn't a bug in Raft at all. The test called `GetState()` directly, in-process, bypassing the simulated network entirely, so an isolated-but-still-running node's own ticker kept it convinced it was leader — which is *correct* per the paper (a partitioned leader legitimately doesn't know it's been deposed until it hears otherwise). The real bug was the test's false equivalence between "unreachable" and "dead." Fixed by adding real `crash()`/`restart()` helpers that actually kill the goroutines and mark the node down, not just sever its network links.

**4. A channel-draining bug produced false "only 2 of 5 committed" failures.** `waitCommitted()` was called twice for the same log index in one test (once for 3 peers, once for all 5) — since Go channels are consumed on read, the first call permanently drained the notification those peers would have needed for the second, broader check. Fixed by tracking already-observed applies per peer across the whole test instead of per call.

**5. Fixed sleeps didn't account for legitimate split votes.** Randomized election timeouts can and do produce two candidates in the same term, neither winning a majority — a normal, paper-documented outcome that just costs one extra election round. A fixed `400ms` sleep before checking for a leader was sometimes too short. Diagnosed by logging every peer's (term, state) every 100ms and watching a real split vote resolve ~500ms in; fixed by polling for a stable leader with a generous timeout instead of guessing a fixed delay.

**6. The real HTTP cluster panicked on every replicated write.** The in-process test suite never had this problem, so it only showed up once `cmd/kvserver` ran as real, separate OS processes talking real HTTP: `raft.LogEntry.Command` is a bare `interface{}`, and `raft/http_transport.go` has to JSON-encode every `AppendEntriesArgs` (including its entries) to actually cross the wire. Go's `encoding/json` has no way to decode an `interface{}` field back into its original concrete struct type - it decodes a JSON object into a generic `map[string]interface{}` instead - so a `kvstore.Op` placed directly in `Command` arrived on the follower as a map, and `applyLoop`'s `msg.Command.(Op)` type assertion panicked the moment any process actually replicated a write over the network. Fixed by having `kvstore.propose()` JSON-encode `Op` to a **string** before ever calling `Start()`: a plain string is exactly the one case `interface{}`-typed JSON decoding *does* recover correctly, so the same code path works unmodified whether the entry crossed a real socket or never left the process. Locked in with two dedicated tests (`kvstore/json_roundtrip_test.go`) - one proving the string encoding survives the exact marshal/unmarshal-into-`interface{}` round trip a real RPC performs, and one proving (so a future refactor can't silently reintroduce the bug) that placing an `Op` directly in the field still does not.

**7. `kvctl get` returned a false "not found."** Found while smoke-testing the real, multi-process HTTP cluster (not the in-process test suite): the CLI queried peers in a fixed order and returned on the very first response, regardless of its `Found` value. Since `Get` is intentionally served from local, possibly-stale state (see below), a follower that hadn't yet replicated the latest write would truthfully answer "not found" for a key that existed on the leader seconds earlier. Fixed so `kvctl get` only reports "not found" once *every* reachable peer has agreed.

## Test suite

```
go test ./...                      # full correctness suite
go test ./... -race -count=5       # same suite under the race detector, repeated
```

`raft/` covers: leader election, log replication, progress with a minority down, *no* progress without a majority (the safety property that actually matters), partition-heal-and-converge, and persistence across a simulated crash/restart. `kvstore/` covers: single-node put/get, replication to all peers, write-survives-leader-failover, and duplicate-proposal de-duplication. `raft/file_persister_test.go` separately unit-tests the real on-disk persistence path (round-trip, full overwrite, and a compile-time check that it satisfies the same interface the in-memory test persister does) — it's simple, deterministic file I/O, verified directly rather than through a flaky multi-process dance. `kvstore/json_roundtrip_test.go` locks in bug #6 above: it proves a command survives the exact JSON-into-`interface{}` round trip a real AppendEntries RPC performs, and that the naive approach it replaced does not. CI additionally builds the real `kvserver`/`kvctl` binaries and runs them as three actual OS processes talking real HTTP, kills the leader mid-run, and asserts the cluster keeps serving writes through a real failover — not just the in-process simulation.

## Known limitations

`KVStore.Get()` reads local state directly without going through Raft, so it's fast and simple but **not linearizable**: a stale or partitioned-away node can serve an old value instead of erroring. A production system would add a `ReadIndex` or leader-lease mechanism for linearizable reads; that's out of scope here and documented rather than silently glossed over. There's no log compaction/snapshotting, so a long-running cluster's log grows unbounded — the paper's §7 snapshotting is a natural next step. Cluster membership is static (set once at startup via `-peers`), not the paper's §6 joint-consensus reconfiguration.

## Tech stack

Go 1.23, standard library only for the algorithm and transport (`net/http`, `encoding/json`) — no consensus or RPC framework. GitHub Actions CI running the full suite plus a real 3-process crash/failover smoke test on every push.
