package pbft

import (
	"slices"
)

type LeaderElection interface {
	GetLeader(view int64) string
}

type RoundRobinLeaderElection struct {
	peerIds []string
}

func NewRoundRobinLeaderElection(config *Config) *RoundRobinLeaderElection {
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
