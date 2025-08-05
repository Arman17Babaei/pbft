package paxos

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Sender struct {
	mu          *sync.RWMutex
	config      *configs.Config
	sendTimeout time.Duration
	maxRetries  int
	otherNodes  map[string]pb.ElectionClient
	pool        *ants.Pool
}

func NewSender(config *configs.Config) *Sender {
	pool, err := ants.NewPool(config.Grpc.MaxConcurrentStreams, ants.WithPreAlloc(true))
	if err != nil {
		log.WithError(err).Fatal("failed to create paxos election pool")
	}

	electionClients := make(map[string]pb.ElectionClient)
	for id, addr := range config.PeersAddress {
		if id == config.Id {
			continue
		}

		c, err := grpc.NewClient(
			fmt.Sprintf("%s:%d", addr.Host, addr.Port+2000),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.WithError(err).Error("failed to create paxos election client")
			continue
		}

		electionClients[id] = pb.NewElectionClient(c)
	}

	return &Sender{
		mu:          &sync.RWMutex{},
		config:      config,
		sendTimeout: time.Duration(config.Grpc.SendTimeoutMs) * time.Millisecond,
		maxRetries:  config.Grpc.MaxRetries,
		otherNodes:  electionClients,
		pool:        pool,
	}
}

func (s *Sender) Broadcast(method string, message proto.Message) {
	log.WithField("method", method).Debug("broadcast paxos election message")
	for id := range s.otherNodes {
		s.SendRPCToPeer(id, method, message)
	}
}

func (s *Sender) SendRPCToPeer(peerID string, method string, message proto.Message) {
	go func() {
		for i := 0; i < s.maxRetries; i++ {
			if err := s.sendRPCToPeer(s.otherNodes[peerID], method, message); err == nil {
				log.WithField("method", method).WithField("peer", peerID).Debug("paxos election message sent")
				monitoring.MessageStatusCounter.WithLabelValues(s.config.Id, peerID, method, "success").Inc()
				return
			} else {
				monitoring.MessageStatusCounter.WithLabelValues(s.config.Id, peerID, method, err.Error()).Inc()
			}
		}
	}()
}

func (s *Sender) sendRPCToPeer(client pb.ElectionClient, method string, message proto.Message) error {
	if client == nil {
		log.Error("paxos election client is nil")
		return fmt.Errorf("nil paxos election client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "PaxosPrepare":
		if _, err := client.PaxosPrepare(ctx, message.(*pb.PaxosPrepareRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("paxos_sender", "SendRPCToPeer-PaxosPrepare", err.Error()).Inc()
			return err
		}
	case "PaxosAccept":
		if _, err := client.PaxosAccept(ctx, message.(*pb.PaxosAcceptRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("paxos_sender", "SendRPCToPeer-PaxosAccept", err.Error()).Inc()
			return err
		}
	case "PaxosLearn":
		if _, err := client.PaxosLearn(ctx, message.(*pb.PaxosLearnRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("paxos_sender", "SendRPCToPeer-PaxosLearn", err.Error()).Inc()
			return err
		}
	default:
		monitoring.ErrorCounter.WithLabelValues("paxos_sender", "SendRPCToPeer-UnknownMethod", method).Inc()
		return fmt.Errorf("unknown paxos election method %s", method)
	}

	return nil
}
