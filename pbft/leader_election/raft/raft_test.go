package raft

import (
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRaftElection_FindLeaderForView(t *testing.T) {
	ctrl := gomock.NewController(t)
	// different configs
	config := &configs.Config{
		Id: "node-0",
		Address: &configs.Address{
			Host: "0.0.0.0",
			Port: 1732,
		},
		PeersAddress: map[string]*configs.Address{
			"node-1": {
				Host: "0.0.0.0",
				Port: 1732,
			},
			// ...
		},
	}

	// 4 of these
	node := NewMockNode(ctrl)
	leaderElection := NewRaftElection(config, node)

	time.Sleep(1 * time.Second)
	callbackChan := make(chan string)
	leaderElection.FindLeaderForView(2, callbackChan)

	electedLeader := <-callbackChan

	// assert all hae elected the same good leader
	assert.Equal(t, "node-0", electedLeader)
}
