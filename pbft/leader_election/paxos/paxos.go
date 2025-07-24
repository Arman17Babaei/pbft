package paxos

//go:generate mockgen -source=paxos.go -destination=paxos_mock.go -package=paxos

import "github.com/Arman17Babaei/pbft/pbft/configs"

type Node interface {
	GetLastStableCheckPoint()
}

type PaxosElection struct {
	config *configs.Config
	node   Node
}

func NewPaxosElection(config *configs.Config, node Node) *PaxosElection {

	return &PaxosElection{
		config: config,
		node:   node,
	}
}

func (r *PaxosElection) FindLeaderForView(viewId int64, callbackCh chan string) {
	// length of the chain
	// start vote -> RequestVote
}
