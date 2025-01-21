package load_tester

import (
	"fmt"
	"github.com/Arman17Babaei/pbft/client"
	"github.com/Arman17Babaei/pbft/config"
	"github.com/Arman17Babaei/pbft/pbft"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"time"
)

type LoadTest struct {
	config  *Config
	clients []*client.Client
	nodes   []*pbft.Node
}

func NewLoadTest(c *Config) *LoadTest {
	l := &LoadTest{config: c}
	var pbftConfig pbft.Config
	err := config.LoadConfig(&pbftConfig, "pbft")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	log.Info("Running cluster")
	for id, address := range pbftConfig.PeersAddress {
		log.WithField("node", id).Info("node configuration")
		configCopy := pbftConfig
		configCopy.Id = id
		configCopy.Address = address
		l.nodes = append(l.nodes, startNode(&configCopy))
	}

	var clientConfig client.Config
	err = config.LoadConfig(&clientConfig, "client")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	for i := 0; i < c.NumClients; i++ {
		configCopy := clientConfig
		configCopy.ClientId = fmt.Sprintf("client-%d", i)
		configCopy.GrpcAddress.Host = "localhost"
		configCopy.GrpcAddress.Port = 3000 + i
		configCopy.HttpAddress = nil
		l.clients = append(l.clients, startClient(&configCopy))
	}

	return l
}

func (l *LoadTest) Run() {
	intervalNs := int64(l.config.NumClients) * time.Second.Nanoseconds() / int64(l.config.Throughput)
	intervalDuration := time.Duration(intervalNs) * time.Nanosecond
	stopChannels := make([]chan struct{}, 0, len(l.clients))
	resultChannels := make([]chan int, 0, len(l.clients))
	time.Sleep(time.Second)
	for _, c := range l.clients {
		stopCh := make(chan struct{}, 1)
		stopChannels = append(stopChannels, stopCh)
		resultCh := make(chan int)
		resultChannels = append(resultChannels, resultCh)
		go func(stopCh chan struct{}, resultCh chan int) {
			ticker := time.NewTicker(intervalDuration)
			responseCh := make(chan *pb.OperationResult)
			done := 0
			for {
				select {
				case <-ticker.C:
					op := &pb.Operation{Type: pb.Operation_GET}
					_ = c.SendRequest(op, responseCh)
				case <-responseCh:
					done += 1
				case <-stopCh:
					resultCh <- done
					break
				}
			}
		}(stopCh, resultCh)
	}
	startTime := time.Now()
	time.Sleep(time.Duration(l.config.DurationSeconds) * time.Second)
	for _, ch := range stopChannels {
		ch <- struct{}{}
		close(ch)
	}
	totalDone := 0
	for _, ch := range resultChannels {
		totalDone += <-ch
	}

	fmt.Printf("Total done: %d\n", totalDone)
	fmt.Printf("Throughput: %f\n", float64(totalDone)/time.Since(startTime).Seconds())
}

func startNode(config *pbft.Config) *pbft.Node {
	inputCh := make(chan proto.Message, 5)
	requestCh := make(chan *pb.ClientRequest, 5)
	service := pbft.NewService(inputCh, requestCh, config)
	sender := pbft.NewSender(config)
	node := pbft.NewNode(config, sender, inputCh, requestCh)

	go node.Run()
	go service.Serve()

	return node
}

func startClient(config *client.Config) *client.Client {
	c := client.NewClient(config)
	httpServer := client.NewHttpServer(config, c)
	go c.Serve()
	go httpServer.Serve()
	return c
}
