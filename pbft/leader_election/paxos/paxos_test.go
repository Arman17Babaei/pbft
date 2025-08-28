package paxos

import (
	"fmt"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPaxos_SuccessfulElection(t *testing.T) {
	const nodeCount = 4
	const viewId = int64(3)

	// Set up
	ctrl := gomock.NewController(t)
	nodeIds := make([]string, 0)
	configStructs := make([]*configs.Config, 0)
	paxosElections := make([]*PaxosElection, 0)
	nodes := make([]*MockNode, 0)
	senders := make([]*Sender, 0)
	for i := range nodeCount {
		nodeIds = append(nodeIds, fmt.Sprintf("node_%d", i+1))
		configStructs = append(configStructs, &configs.Config{
			Id: nodeIds[i],
			PeersAddress: map[string]*configs.Address{
				"node_1": {
					Host: "localhost",
					Port: 1001,
				},
				"node_2": {
					Host: "localhost",
					Port: 1002,
				},
				"node_3": {
					Host: "localhost",
					Port: 1003,
				},
				"node_4": {
					Host: "localhost",
					Port: 1004,
				},
			},
			Timers: &configs.Timers{
				ViewChangeTimeoutMs: 10_000,
			},
		})
		senders = append(senders, NewSender(configStructs[i]))
		nodes = append(nodes, NewMockNode(ctrl))
		nodes[i].EXPECT().GetCurrentView().Return(int64(1)).Times(1)
		nodes[i].EXPECT().GetCurrentViewLeader().Return("node_1").Times(1)
		paxosElections = append(paxosElections, NewPaxosElection(configStructs[i], nodes[i], senders[i]))
		err := paxosElections[i].Start()
		assert.NoError(t, err)
	}

	// Act
	resultChannels := make([]chan string, 0)
	for i, election := range paxosElections {
		resultChannels = append(resultChannels, make(chan string, 1))
		election.FindLeaderForView(viewId, resultChannels[i])
		time.Sleep(10 * time.Millisecond)
	}
	leaders := make([]string, 0)
	for _, ch := range resultChannels {
		leaders = append(leaders, <-ch)
	}

	// Assert
	assert.Contains(t, nodeIds, leaders[0], "bad leader elected")
	for _, leader := range leaders {
		assert.Equal(t, leaders[0], leader)
	}

	fmt.Printf("Elected %s as leader\n", leaders[0])

	// Clean Up
	for _, election := range paxosElections {
		err := election.Stop()
		assert.NoError(t, err)
	}
}
