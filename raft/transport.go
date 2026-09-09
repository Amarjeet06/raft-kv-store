package raft

// Transport abstracts how a Raft peer sends RequestVote and AppendEntries
// RPCs to another peer. The test suite uses the in-process simulated
// Network (network.go) to inject faults without real sockets; a real
// deployment (cmd/kvserver) uses HTTPTransport (http_transport.go) to
// actually talk to other processes over a real network connection.
type Transport interface {
	SendRequestVote(from, to int, args *RequestVoteArgs) (*RequestVoteReply, bool)
	SendAppendEntries(from, to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool)
}
