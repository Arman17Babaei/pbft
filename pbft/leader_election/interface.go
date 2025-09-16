package leader_election

import "google.golang.org/protobuf/proto"

// General Leader Election Interface
// To Support Different Leader Election Algorithms
// For Example: RoundRobin, Multi-Paxos, Raft, etc.
type LeaderElection interface {
	// Basic Function (To Support Function in other modules)
	// For Synchronous Function (Predefined Leader Order), like RoundRobin
	GetLeader() string
	FindLeaderForView(viewId int64, callbackCh chan string)

	// For Consensus-based Leader Election (need internal RPC service)
	// Like Multi-Paxos, Raft, etc.
	Start() error
	Stop() error
	HandleMessage(msg proto.Message) error
	GetCurrentLeader() string
	IsLeader() bool
}

type ISender interface {
	SendRPCToPeer(peerID string, method string, message proto.Message)
	Broadcast(method string, message proto.Message)
}
