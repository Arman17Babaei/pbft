package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/Arman17Babaei/pbft/grpc/proto"
	"github.com/Arman17Babaei/pbft/pbft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PbftService struct {
	mu       sync.Mutex
	node     *pbft.Node
	pleaseCh chan *pb.RequestPleaseRequest

	pb.UnimplementedRaftServer
}

func NewPbftService(node *pbft.Node, port int, pleaseCh chan *pb.RequestPleaseRequest) *PbftService {
	pbftService := &PbftService{node: node, pleaseCh: pleaseCh}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpc.WithTransportCredentials(insecure.NewCredentials())
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, pbftService)

	go func() {
		fmt.Printf("Starting gRPC server on port %d...", port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	return pbftService
}
func (rs *PbftService) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	fmt.Printf("AppendEntries received: %+v\n", req)

	return rs.node.AppendEntriesHandler(req)
}

func (rs *PbftService) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	fmt.Printf("RequestVote received: %+v\n", req)

	return rs.node.RequestVoteHandler(req)
}

func (rs *PbftService) PleaseDoThis(ctx context.Context, req *pb.RequestPleaseRequest) (*pb.RequestPleaseResponse, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	fmt.Printf("PleaseDoThis received: %+v\n", req)

	rs.pleaseCh <- req

	return &pb.RequestPleaseResponse{Success: true}, nil
}
