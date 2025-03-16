package pbft

import (
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"slices"
)

type LeaderElection interface {
	GetLeader(view int64) string
}

type RoundRobinLeaderElection struct {
	peerIds []string
}

func NewRoundRobinLeaderElection(config *configs.Config) *RoundRobinLeaderElection {
	leaderIds := make([]string, 0)
	for id := range config.PeersAddress {
		leaderIds = append(leaderIds, id)
	}
	slices.Sort(leaderIds)

	return &RoundRobinLeaderElection{
		peerIds: leaderIds,
	}
}

func (r *RoundRobinLeaderElection) GetLeader(view int64) string {
	return r.peerIds[view%int64(len(r.peerIds))]
}
