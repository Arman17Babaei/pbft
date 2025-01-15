package pbft

import (
	"context"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"

	pb "github.com/Arman17Babaei/pbft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	config     *Config
	inputCh    chan<- proto.Message
	listener   net.Listener
	grpcServer *grpc.Server

	pb.UnimplementedPbftServer
}

func NewService(requestCh chan<- proto.Message, config *Config) *Service {
	service := &Service{config: config, inputCh: requestCh}

	var err error
	service.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	grpc.WithTransportCredentials(insecure.NewCredentials())
	service.grpcServer = grpc.NewServer()
	pb.RegisterPbftServer(service.grpcServer, service)

	return service
}

func (s *Service) Serve() {
	log.WithFields(log.Fields{"host": s.config.Address.Host, "port": s.config.Address.Port}).Printf("Starting gRPC server...")
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}

func (s *Service) Request(ctx context.Context, req *pb.ClientRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("client request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) PrePrepare(ctx context.Context, req *pb.PiggyBackedPrePareRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("pre-prepare request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("prepare request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("commit request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Checkpoint(ctx context.Context, req *pb.CheckpointRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("checkpoint request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) ViewChange(ctx context.Context, req *pb.ViewChangeRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("view-change request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) NewView(ctx context.Context, req *pb.NewViewRequest) (*pb.Empty, error) {
	log.WithField("request", req).Info("new-view request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}
