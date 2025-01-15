package pbft

import (
	"context"
	"fmt"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"

	pb "github.com/Arman17Babaei/pbft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Sender struct {
	sendTimeout time.Duration
	peers       map[string]*Address
	pool        *ants.Pool
}

func NewSender(config *Config) *Sender {
	pool, err := ants.NewPool(config.Grpc.MaxConcurrentStreams, ants.WithPreAlloc(true))
	if err != nil {
		log.WithError(err).Fatal("failed to create pool")
	}
	peers := make(map[string]*Address)
	for id, addr := range config.PeersAddress {
		if id != config.Id {
			peers[id] = addr
		}
	}

	return &Sender{
		sendTimeout: time.Duration(config.Grpc.SendTimeoutMs) * time.Millisecond,
		peers:       peers,
		pool:        pool,
	}
}

func (s *Sender) Broadcast(method string, message proto.Message) {
	log.WithField("method", method).Debug("broadcast message")
	for id := range s.peers {
		s.SendRPCToPeer(id, method, message)
	}
}

func (s *Sender) SendRPCToPeer(peerID string, method string, message proto.Message) {
	ants.Submit(func() {
		s.sendRPCToPeer(s.peers[peerID], method, message)
	})
}

func (s *Sender) SendRPCToClient(clientAddress, method string, message proto.Message) {
	ants.Submit(func() {
		s.sendRPCToClient(clientAddress, method, message)
	})
}

func (s *Sender) sendRPCToClient(clientAddress, method string, message proto.Message) {
	c, err := grpc.NewClient(
		clientAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.WithError(err).Error("failed to create client")
		return
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "Response":
		if _, err := pb.NewClientClient(c).Response(ctx, message.(*pb.ClientResponse)); err != nil {
			log.WithError(err).Error("failed to send Reply")
		}
	default:
		log.Error("unknown method")
	}
}

func (s *Sender) sendRPCToPeer(peerAddress *Address, method string, message proto.Message) {
	c, err := grpc.NewClient(
		fmt.Sprintf("%s:%d", peerAddress.Host, peerAddress.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.WithError(err).Error("failed to create client")
		return
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "Request":
		if _, err := pb.NewPbftClient(c).Request(ctx, message.(*pb.ClientRequest)); err != nil {
			log.WithError(err).Error("failed to send Request")
		}
	case "PrePrepare":
		if _, err := pb.NewPbftClient(c).PrePrepare(ctx, message.(*pb.PiggyBackedPrePareRequest)); err != nil {
			log.WithError(err).Error("failed to send PrePrepare")
		}
	case "Prepare":
		if _, err := pb.NewPbftClient(c).Prepare(ctx, message.(*pb.PrepareRequest)); err != nil {
			log.WithError(err).Error("failed to send Prepare")
		}
	case "Commit":
		if _, err := pb.NewPbftClient(c).Commit(ctx, message.(*pb.CommitRequest)); err != nil {
			log.WithError(err).Error("failed to send Commit")
		}
	case "Checkpoint":
		if _, err := pb.NewPbftClient(c).Checkpoint(ctx, message.(*pb.CheckpointRequest)); err != nil {
			log.WithError(err).Error("failed to send Checkpoint")
		}
	case "ViewChange":
		if _, err := pb.NewPbftClient(c).ViewChange(ctx, message.(*pb.ViewChangeRequest)); err != nil {
			log.WithError(err).Error("failed to send ViewChange")
		}
	case "NewView":
		if _, err := pb.NewPbftClient(c).NewView(ctx, message.(*pb.NewViewRequest)); err != nil {
			log.WithError(err).Error("failed to send NewView")
		}
	default:
		log.Error("unknown method")
	}
}
