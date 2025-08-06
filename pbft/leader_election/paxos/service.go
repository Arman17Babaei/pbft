package paxos

import (
	"context"
	"fmt"
	"net"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Service represents the gRPC server for Paxos election protocol
// It handles incoming Paxos messages and forwards them to the election logic
type Service struct {
	config     *configs.Config
	paxosCh    chan<- proto.Message
	listener   net.Listener
	grpcServer *grpc.Server

	Enabled bool

	pb.UnimplementedElectionServer
}

// NewService creates a new Paxos election service
// It listens on port + 2000 (following the pattern: main port + 1000 for view change, + 2000 for election)
func NewService(paxosCh chan<- proto.Message, config *configs.Config) *Service {
	service := &Service{
		config:  config,
		paxosCh: paxosCh,
		Enabled: config.General.EnabledByDefault,
	}

	var err error
	// Listen on port + 2000 for election service
	service.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port+2000))
	if err != nil {
		log.WithError(err).Fatal("failed to listen for paxos election service")
	}

	grpc.WithTransportCredentials(insecure.NewCredentials())
	service.grpcServer = grpc.NewServer()
	pb.RegisterElectionServer(service.grpcServer, service)

	return service
}

// Serve starts the gRPC server for Paxos election
func (s *Service) Serve() {
	log.WithFields(log.Fields{
		"host": s.config.Address.Host,
		"port": s.config.Address.Port + 2000,
	}).Printf("Starting paxos election gRPC server...")

	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.WithError(err).Fatal("failed to serve paxos election")
	}
}

// PaxosPrepare handles incoming Paxos Prepare phase requests
func (s *Service) PaxosPrepare(_ context.Context, req *pb.PaxosPrepareRequest) (*pb.PaxosPrepareResponse, error) {
	if !s.Enabled {
		return &pb.PaxosPrepareResponse{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Info("paxos prepare request received")

	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "paxos-prepare").Inc()

	return &pb.PaxosPrepareResponse{}, nil
}

// PaxosAccept handles incoming Paxos Accept phase requests
func (s *Service) PaxosAccept(_ context.Context, req *pb.PaxosAcceptRequest) (*pb.PaxosAcceptResponse, error) {
	if !s.Enabled {
		return &pb.PaxosAcceptResponse{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Info("paxos accept request received")

	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "paxos-accept").Inc()

	return &pb.PaxosAcceptResponse{}, nil
}

// PaxosLearn handles incoming Paxos Learn phase requests
func (s *Service) PaxosLearn(_ context.Context, req *pb.PaxosLearnRequest) (*pb.PaxosLearnResponse, error) {
	if !s.Enabled {
		return &pb.PaxosLearnResponse{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Info("paxos learn request received")

	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "paxos-learn").Inc()

	return &pb.PaxosLearnResponse{}, nil
}

// GetElectionStatus handles election status requests
func (s *Service) GetElectionStatus(_ context.Context, req *pb.ElectionStatusRequest) (*pb.ElectionStatusResponse, error) {
	if !s.Enabled {
		return &pb.ElectionStatusResponse{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("request-from", req.NodeId).
		Info("election status request received")

	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetNodeId(), s.config.Id, "election-status").Inc()

	return &pb.ElectionStatusResponse{}, nil
}

// Enable enables the election service
func (s *Service) Enable(_ context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.WithField("my-id", s.config.Id).Info("enable paxos election request received")
	s.Enabled = true
	monitoring.MessageCounter.WithLabelValues("admin", s.config.Id, "enable-election").Inc()

	return &pb.Empty{}, nil
}

// Disable disables the election service
func (s *Service) Disable(_ context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.WithField("my-id", s.config.Id).Info("disable paxos election request received")
	s.Enabled = false
	monitoring.MessageCounter.WithLabelValues("admin", s.config.Id, "disable-election").Inc()

	return &pb.Empty{}, nil
}
