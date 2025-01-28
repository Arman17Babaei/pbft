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
	requestCh  chan<- *pb.ClientRequest
	enableCh   chan<- any
	disableCh  chan<- any
	listener   net.Listener
	grpcServer *grpc.Server

	Enabled bool

	pb.UnimplementedPbftServer
}

func NewService(inputCh chan<- proto.Message, requestCh chan<- *pb.ClientRequest, enableCh chan<- any, disableCh chan<- any, config *Config) *Service {
	service := &Service{
		config:    config,
		inputCh:   inputCh,
		requestCh: requestCh,
		enableCh:  enableCh,
		disableCh: disableCh,
		Enabled:   config.General.EnabledByDefault,
	}

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

func (s *Service) Request(_ context.Context, req *pb.ClientRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("client request received")
	putOrIgnore(s.requestCh, req)

	return &pb.Empty{}, nil
}

func (s *Service) PrePrepare(_ context.Context, req *pb.PiggyBackedPrePareRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("pre-prepare request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Prepare(_ context.Context, req *pb.PrepareRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("prepare request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Commit(_ context.Context, req *pb.CommitRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("commit request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Checkpoint(_ context.Context, req *pb.CheckpointRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("checkpoint request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) ViewChange(_ context.Context, req *pb.ViewChangeRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("view-change request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) NewView(_ context.Context, req *pb.NewViewRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("request", req).Info("new-view request received")
	s.inputCh <- req

	return &pb.Empty{}, nil
}

func (s *Service) Enable(_ context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.Info("enable request received")
	putOrIgnore[any](s.disableCh, req)

	s.Enabled = true

	return &pb.Empty{}, nil
}

func (s *Service) Disable(_ context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.Info("disable request received")
	putOrIgnore[any](s.disableCh, req)

	s.Enabled = false

	return &pb.Empty{}, nil
}

func putOrIgnore[T any](channel chan<- T, value T) bool {
	select {
	case channel <- value:
		return true
	default:
		return false
	}
}
