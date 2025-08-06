package paxosx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Sender struct {
	config *configs.Config
	conns  map[string]*grpc.ClientConn
	mu     sync.RWMutex
}

// proto:
// PaxosClient
// NewPaxosClient

func NewSender(config *configs.Config) *Sender {
	return &Sender{
		config: config,
		conns:  make(map[string]*grpc.ClientConn),
		mu:     sync.RWMutex{}, // Non-Necessary, 0-value is fine
	}
}

func (s *Sender) getClient(id string) (pb.PaxosElectionClient, error) {
	// 1 Check Whether Connection Exists, if so, return the client
	s.mu.RLock()
	conn, ok := s.conns[id]
	s.mu.RUnlock()
	if ok {
		return pb.NewPaxosElectionClient(conn), nil
	}

	// 2 If Not, Establish Connection
	s.mu.Lock()
	defer s.mu.Unlock()

	// 2.1 Double Check Whether Connection Exists
	// As in Execution Interval, Other Threads might have established the connection, so we need to check again
	if conn, ok = s.conns[id]; ok {
		return pb.NewPaxosElectionClient(conn), nil
	}

	// 2.2 Establish Connection
	address := s.config.GetAddress(id)
	if address == nil {
		return nil, fmt.Errorf("no address found for replica %s", id)
	}
	// Specific to Leader Election Port (Original Port + 2000)
	address.Port += 2000

	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:%d", address.Host, address.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to replica %s: %v", id, err)
	}

	s.conns[id] = conn
	return pb.NewPaxosElectionClient(conn), nil
}

func (s *Sender) SendPaxosPrepare(targetId string, req *pb.PaxosPrepareRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.PaxosPrepare(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send paxos-prepare request")
		monitoring.ErrorCounter.WithLabelValues("paxos-prepare", "SendPaxosPrepare", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendPaxosAccept(targetId string, req *pb.PaxosAcceptRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.PaxosAccept(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send paxos-accept request")
		monitoring.ErrorCounter.WithLabelValues("paxos-accept", "SendPaxosAccept", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendPaxosSuccess(targetId string, req *pb.PaxosSuccessRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.PaxosSuccess(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send paxos-success request")
		monitoring.ErrorCounter.WithLabelValues("paxos-success", "SendPaxosSuccess", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendPaxosPromise(targetId string, req *pb.PaxosPromiseRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.PaxosPromise(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send paxos-promise request")
		monitoring.ErrorCounter.WithLabelValues("paxos-promise", "SendPaxosPromise", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) Broadcast(msgType string, message proto.Message) error {
	for _, replicaId := range s.config.ReplicaIds() {
		if replicaId == s.config.Id {
			continue
		}
		switch msgType {
		case "paxos-prepare":
			s.SendPaxosPrepare(replicaId, message.(*pb.PaxosPrepareRequest))
		case "paxos-accept":
			s.SendPaxosAccept(replicaId, message.(*pb.PaxosAcceptRequest))
		default:
			return fmt.Errorf("invalid message type: %s", msgType)
		}
	}
	return nil
}

func (s *Sender) SendRPCToPeer(id string, msgType string, message proto.Message) error {
	switch msgType {
	case "paxos-prepare":
		s.SendPaxosPrepare(id, message.(*pb.PaxosPrepareRequest))
	case "paxos-promise":
		s.SendPaxosPromise(id, message.(*pb.PaxosPromiseRequest))
	case "paxos-accept":
		s.SendPaxosAccept(id, message.(*pb.PaxosAcceptRequest))
	case "paxos-success":
		s.SendPaxosSuccess(id, message.(*pb.PaxosSuccessRequest))
	default:
		return fmt.Errorf("invalid message type: %s", msgType)
	}
	return nil
}
