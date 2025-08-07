package view_changer

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

type Service struct {
	config       *configs.Config
	viewChangeCh chan<- proto.Message
	listener     net.Listener
	grpcServer   *grpc.Server

	Enabled bool

	pb.UnimplementedViewChangerServer
}

func NewService(viewChangeCh chan<- proto.Message, config *configs.Config) *Service {
	service := &Service{
		config:       config,
		viewChangeCh: viewChangeCh,
		Enabled:      config.General.EnabledByDefault,
	}

	var err error
	service.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port+1000))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	grpc.WithTransportCredentials(insecure.NewCredentials())
	service.grpcServer = grpc.NewServer()
	pb.RegisterViewChangerServer(service.grpcServer, service)

	return service
}

func (s *Service) Serve() {
	log.WithFields(log.Fields{"host": s.config.Address.Host, "port": s.config.Address.Port + 1000}).Printf("Starting view change gRPC server...")
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}

func (s *Service) ViewChange(_ context.Context, req *pb.ViewChangeRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).WithField("new-view", req.GetNewViewId()).Info("view-change request received")
	s.viewChangeCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetReplicaId(), s.config.Id, "view-change").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) NewView(_ context.Context, req *pb.NewViewRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).WithField("new-view-id", req.NewViewId).WithField("leader-id", req.ReplicaId).Info("new-view request received")
	s.viewChangeCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetReplicaId(), s.config.Id, "new-view").Inc()

	return &pb.Empty{}, nil
}
