package leader_election

import (
	pb "github.com/Arman17Babaei/pbft/proto"
	"slices"

	"github.com/Arman17Babaei/pbft/pbft/configs"
)

type RoundRobin struct {
	peerIds     []string
	ViewChanges map[int64]map[string]*pb.ViewChangeRequest
}

func NewRoundRobinLeaderElection(config *configs.Config) *RoundRobin {
	leaderIds := make([]string, 0)
	for id := range config.PeersAddress {
		leaderIds = append(leaderIds, id)
	}
	slices.Sort(leaderIds)

	return &RoundRobin{
		peerIds: leaderIds,
	}
}

func (r *RoundRobin) GetLeader(view int64) string {
	return r.peerIds[view%int64(len(r.peerIds))]
}
