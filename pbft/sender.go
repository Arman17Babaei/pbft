package pbft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"

	pb "github.com/Arman17Babaei/pbft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Sender struct {
	mu          *sync.RWMutex
	sendTimeout time.Duration
	maxRetries  int
	clients     map[string]pb.ClientClient
	pbftClients map[string]pb.PbftClient
	pool        *ants.Pool
}

func NewSender(config *Config) *Sender {
	pool, err := ants.NewPool(config.Grpc.MaxConcurrentStreams, ants.WithPreAlloc(true))
	if err != nil {
		log.WithError(err).Fatal("failed to create pool")
	}
	pbftClients := make(map[string]pb.PbftClient)
	for id, addr := range config.PeersAddress {
		if id == config.Id {
			continue
		}

		c, err := grpc.NewClient(
			fmt.Sprintf("%s:%d", addr.Host, addr.Port),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.WithError(err).Error("failed to create client")
		}

		pbftClients[id] = pb.NewPbftClient(c)
	}

	return &Sender{
		mu:          &sync.RWMutex{},
		sendTimeout: time.Duration(config.Grpc.SendTimeoutMs) * time.Millisecond,
		maxRetries:  config.Grpc.MaxRetries,
		clients:     make(map[string]pb.ClientClient),
		pbftClients: pbftClients,
		pool:        pool,
	}
}

func (s *Sender) Broadcast(method string, message proto.Message) {
	log.WithField("method", method).Debug("broadcast message")
	for id := range s.pbftClients {
		s.SendRPCToPeer(id, method, message)
	}
}

func (s *Sender) SendRPCToPeer(peerID string, method string, message proto.Message) {
	go func() {
		for i := 0; i < s.maxRetries; i++ {
			if s.sendRPCToPeer(s.pbftClients[peerID], method, message) {
				log.WithField("method", method).WithField("peer", peerID).Debug("message sent")
				return
			}
		}
	}()
}

func (s *Sender) SendRPCToClient(clientAddress, method string, message proto.Message) {
	_ = s.pool.Submit(func() {
		s.sendRPCToClient(clientAddress, method, message)
	})
}

func (s *Sender) sendRPCToClient(clientAddress, method string, message proto.Message) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.clients[clientAddress]; !ok {
		var err error
		c, err := grpc.NewClient(
			clientAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.WithError(err).Error("failed to create client")
			return
		}

		s.mu.RUnlock()
		s.mu.Lock()
		s.clients[clientAddress] = pb.NewClientClient(c)
		s.mu.Unlock()
		s.mu.RLock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "Response":
		if _, err := s.clients[clientAddress].Response(ctx, message.(*pb.ClientResponse)); err != nil {
			log.WithError(err).Warn("failed to send Reply")
		}
	default:
		log.Error("unknown method")
	}
}

func (s *Sender) sendRPCToPeer(client pb.PbftClient, method string, message proto.Message) bool {
	if client == nil {
		log.Error("peer address is nil")
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "Request":
		if _, err := client.Request(ctx, message.(*pb.ClientRequest)); err != nil {
			log.WithError(err).Info("failed to send Request")
			return false
		}
	case "PrePrepare":
		if _, err := client.PrePrepare(ctx, message.(*pb.PiggyBackedPrePareRequest)); err != nil {
			log.WithError(err).Error("failed to send PrePrepare")
			return false
		}
	case "Prepare":
		if _, err := client.Prepare(ctx, message.(*pb.PrepareRequest)); err != nil {
			log.WithError(err).Error("failed to send Prepare")
			return false
		}
	case "Commit":
		if _, err := client.Commit(ctx, message.(*pb.CommitRequest)); err != nil {
			log.WithError(err).Error("failed to send Commit")
			return false
		}
	case "Checkpoint":
		if _, err := client.Checkpoint(ctx, message.(*pb.CheckpointRequest)); err != nil {
			log.WithError(err).Error("failed to send Checkpoint")
			return false
		}
	case "ViewChange":
		if _, err := client.ViewChange(ctx, message.(*pb.ViewChangeRequest)); err != nil {
			log.WithError(err).Error("failed to send ViewChange")
			return false
		}
	case "NewView":
		if _, err := client.NewView(ctx, message.(*pb.NewViewRequest)); err != nil {
			log.WithError(err).Error("failed to send NewView")
			return false
		}
	case "GetStatus":
		if _, err := client.GetStatus(ctx, message.(*pb.StatusRequest)); err != nil {
			log.WithError(err).Error("failed to send GetStatus")
			return false
		}
	case "Status":
		if _, err := client.Status(ctx, message.(*pb.StatusResponse)); err != nil {
			log.WithError(err).Error("failed to send Status")
			return false
		}
	default:
		log.Error("unknown method")
		return false
	}

	return true
}
