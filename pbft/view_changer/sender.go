package view_changer

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

func NewSender(config *configs.Config) *Sender {
	return &Sender{
		config: config,
		conns:  make(map[string]*grpc.ClientConn),
	}
}

func (s *Sender) getClient(id string) (pb.ViewChangerClient, error) {
	s.mu.RLock()
	conn, ok := s.conns[id]
	s.mu.RUnlock()
	if ok {
		return pb.NewViewChangerClient(conn), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double check after acquiring write lock
	if conn, ok = s.conns[id]; ok {
		return pb.NewViewChangerClient(conn), nil
	}

	address := s.config.GetAddress(id)
	if address == nil {
		return nil, fmt.Errorf("no address found for replica %s", id)
	}

	// Add 1000 to port for view change service
	address.Port += 1000

	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:%d", address.Host, address.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to replica %s: %v", id, err)
	}

	s.conns[id] = conn
	return pb.NewViewChangerClient(conn), nil
}

func (s *Sender) SendViewChange(targetId string, req *pb.ViewChangeRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.ViewChange(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send view change request")
		monitoring.ErrorCounter.WithLabelValues("view_changer", "SendViewChange", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendNewView(targetId string, req *pb.NewViewRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.NewView(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send new view request")
		monitoring.ErrorCounter.WithLabelValues("view_changer", "SendNewView", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) Broadcast(msgType string, msg proto.Message) {
	for _, replicaId := range s.config.ReplicaIds() {
		if replicaId == s.config.Id {
			continue
		}

		switch msgType {
		case "ViewChange":
			if req, ok := msg.(*pb.ViewChangeRequest); ok {
				err := s.SendViewChange(replicaId, req)
				if err != nil {
					log.WithError(err).WithField("target", replicaId).Error("failed to broadcast view change request")
				}
			}
		case "NewView":
			if req, ok := msg.(*pb.NewViewRequest); ok {
				err := s.SendNewView(replicaId, req)
				if err != nil {
					log.WithError(err).WithField("target", replicaId).Error("failed to broadcast new view request")
				}
			}
		}
	}
}

func (s *Sender) SendRPCToPeer(peerID string, method string, message proto.Message) {
	switch method {
	case "ViewChange":
		if req, ok := message.(*pb.ViewChangeRequest); ok {
			err := s.SendViewChange(peerID, req)
			if err != nil {
				log.WithError(err).WithField("target", peerID).Error("failed to send view change request")
			}
		}
	case "NewView":
		if req, ok := message.(*pb.NewViewRequest); ok {
			err := s.SendNewView(peerID, req)
			if err != nil {
				log.WithError(err).WithField("target", peerID).Error("failed to send new view request")
			}
		}
	}
}
