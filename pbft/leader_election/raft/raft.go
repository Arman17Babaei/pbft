package raft

//go:generate mockgen -source=raft.go -destination=raft_mock.go -package=raft

import "github.com/Arman17Babaei/pbft/pbft/configs"

type Node interface {
	GetLastStableCheckPoint()
}

type RaftElection struct {
	config *configs.Config
	node   Node
}

func NewRaftElection(config *configs.Config, node Node) *RaftElection {
	return &RaftElection{
		config: config,
		node:   node,
	}
}

func (r *RaftElection) FindLeaderForView(viewId int64, callbackCh chan string) {
	// length of the chain
	// start vote -> RequestVote
}
