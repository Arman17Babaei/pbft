package scenarios

import (
	"context"
	"fmt"
	"github.com/Arman17Babaei/pbft/load_tester/config"
	"github.com/Arman17Babaei/pbft/pbft"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type PeriodicFailure struct {
	peersAddress map[string]*pbft.Address
	nodes        map[string]pb.PbftClient
	interval     time.Duration
}

func (p *PeriodicFailure) PrepareScenario(loadTestConfig *config.Config, pbftConfig *pbft.Config) {
	p.peersAddress = pbftConfig.PeersAddress
	p.interval = time.Duration(loadTestConfig.PeriodicFailure.IntervalSeconds) * time.Second
	p.nodes = make(map[string]pb.PbftClient)
	for id, address := range p.peersAddress {
		target := fmt.Sprintf("%s:%d", address.Host, address.Port)
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		p.nodes[id] = pb.NewPbftClient(conn)
	}
}

func (p *PeriodicFailure) Run(stopCh <-chan any) {
	nodeIds := make([]string, 0, len(p.nodes))
	for id := range p.nodes {
		nodeIds = append(nodeIds, id)
	}

	toggleTicker := time.NewTicker(p.interval)
	curIdx := len(nodeIds) - 1
	for {
		select {
		case <-stopCh:
			return
		case <-toggleTicker.C:
			sendEnable(p.nodes[nodeIds[curIdx]])
			curIdx = (curIdx + 1) % len(nodeIds)
			log.WithField("node id", nodeIds[curIdx]).Error("Disabling")
			sendDisable(p.nodes[nodeIds[curIdx]])
		}
	}
}

func sendEnable(node pb.PbftClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := node.Enable(ctx, &pb.Empty{})
	if err != nil {
		log.WithError(err).Error("failed to enable node")
	}
}

func sendDisable(node pb.PbftClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := node.Disable(ctx, &pb.Empty{})
	if err != nil {
		log.WithError(err).Error("failed to enable node")
	}
}
