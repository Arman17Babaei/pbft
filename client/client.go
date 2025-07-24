package client

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	mu        sync.Mutex
	config    *Config
	nodeNames []string
	nodeConns map[string]pb.PbftClient

	listener          net.Listener
	grpcServer        *grpc.Server
	callbackAddress   string
	collectedResponse map[int64]*ResponseCollection

	callbackChannels map[int64]chan<- *pb.OperationResult
	currentLeader    string

	pb.UnimplementedClientServer
}

type ResponseCollection struct {
	rcmu       sync.RWMutex
	collection map[string]map[string]*pb.ClientResponse
}

func NewResponseCollection() *ResponseCollection {
	return &ResponseCollection{
		collection: make(map[string]map[string]*pb.ClientResponse),
	}
}

func (rc *ResponseCollection) AddResponse(response *pb.ClientResponse) {
	rc.rcmu.Lock()
	defer rc.rcmu.Unlock()

	if _, ok := rc.collection[response.Result.Value]; !ok {
		rc.collection[response.Result.Value] = make(map[string]*pb.ClientResponse)
	}
	rc.collection[response.Result.Value][response.ReplicaId] = response
}

func (rc *ResponseCollection) GetSize(_ string) int {
	rc.rcmu.RLock()
	defer rc.rcmu.RUnlock()

	maxLen := 0
	for _, responses := range rc.collection {
		if len(responses) > maxLen {
			maxLen = len(responses)
		}
	}

	return maxLen
}

func (rc *ResponseCollection) GetResponse() *pb.ClientResponse {
	rc.rcmu.RLock()
	defer rc.rcmu.RUnlock()

	maxLen := 0
	var response *pb.ClientResponse
	for _, responses := range rc.collection {
		if len(responses) > maxLen {
			maxLen = len(responses)
			for _, r := range responses {
				response = r
				break
			}
		}
	}

	return response
}

func NewClient(config *Config) *Client {
	client := &Client{
		config:            config,
		nodeConns:         make(map[string]pb.PbftClient),
		collectedResponse: make(map[int64]*ResponseCollection),
		callbackChannels:  make(map[int64]chan<- *pb.OperationResult),
	}

	// setup server
	client.callbackAddress = fmt.Sprintf("%s:%d", config.GrpcAddress.Host, config.GrpcAddress.Port)

	var err error
	client.listener, err = net.Listen("tcp", client.callbackAddress)
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	client.grpcServer = grpc.NewServer(grpc.MaxConcurrentStreams(100))
	pb.RegisterClientServer(client.grpcServer, client)

	client.nodeNames = make([]string, 0, len(config.NodesAddress))
	for id := range config.NodesAddress {
		client.nodeNames = append(client.nodeNames, id)
	}
	sort.Strings(client.nodeNames)

	client.currentLeader = client.nodeNames[rand.Intn(len(client.nodeNames))]

	for id, node := range config.NodesAddress {
		target := fmt.Sprintf("%s:%d", node.Host, node.Port)
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.WithError(err).WithField("target", target).Error("error creating pbft client")
		}

		client.nodeConns[id] = pb.NewPbftClient(conn)
	}

	return client
}

func (c *Client) Serve() {
	log.WithField("target", c.callbackAddress).Printf("Starting gRPC server...")
	if err := c.grpcServer.Serve(c.listener); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}

func (c *Client) SendRequest(op *pb.Operation, callback chan<- *pb.OperationResult) error {
	log.Debug("client.sendRequest")
	c.mu.Lock()
	leaderClient := c.nodeConns[c.currentLeader]
	c.currentLeader = c.nodeNames[rand.Intn(len(c.nodeNames))]

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.config.GrpcTimeoutMs)*time.Millisecond)
	defer cancel()

	timestamp := time.Now().UnixNano()
	clientRequest := &pb.ClientRequest{
		ClientId:    c.config.ClientId,
		TimestampNs: timestamp,
		Operation:   op,
		Callback:    c.callbackAddress,
	}

	c.callbackChannels[timestamp] = callback
	c.mu.Unlock()

	_, err := leaderClient.Request(ctx, clientRequest)
	if err != nil {
		log.WithError(err).Info("error sending request to leader")
		return err
	}

	log.WithField("leader", c.currentLeader).Info("request sent to leader")
	return nil
}

func (c *Client) Response(_ context.Context, response *pb.ClientResponse) (*pb.Empty, error) {
	log.Debug("client.response")
	log.WithField("reponse", response.String()).Info("operation result received")
	c.mu.Lock()
	if _, ok := c.collectedResponse[response.TimestampNs]; !ok {
		c.collectedResponse[response.TimestampNs] = NewResponseCollection()
	}

	responseCollection := c.collectedResponse[response.TimestampNs]
	operationResults := c.callbackChannels[response.TimestampNs]
	c.mu.Unlock()

	responseCollection.AddResponse(response)

	if responseCollection.GetSize(response.Result.Value) == c.config.F()+1 {
		log.WithField("timestamp", response.TimestampNs).Info("request response ready")
		operationResults <- responseCollection.GetResponse().Result
	}

	return &pb.Empty{}, nil
}
