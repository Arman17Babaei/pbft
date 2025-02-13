package load_tester

import (
	"fmt"
	"github.com/Arman17Babaei/pbft/client"
	loader "github.com/Arman17Babaei/pbft/config"
	loadtestconfig "github.com/Arman17Babaei/pbft/load_tester/config"
	"github.com/Arman17Babaei/pbft/load_tester/scenarios"
	"github.com/Arman17Babaei/pbft/pbft"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"time"
)

type LoadTest struct {
	config     *loadtestconfig.Config
	pbftConfig *pbft.Config
	clients    []*client.Client
	nodes      []*pbft.Node
}

func NewLoadTest(c *loadtestconfig.Config) *LoadTest {
	l := &LoadTest{config: c}
	var pbftConfig pbft.Config
	err := loader.LoadConfig(&pbftConfig, "pbft")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}
	l.pbftConfig = &pbftConfig

	log.Info("Running cluster")
	for id, address := range pbftConfig.PeersAddress {
		log.WithField("node", id).Info("node configuration")
		configCopy := pbftConfig
		configCopy.Id = id
		configCopy.Address = address
		l.nodes = append(l.nodes, startNode(&configCopy))
	}

	var clientConfig client.Config
	err = loader.LoadConfig(&clientConfig, "client")
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
	stopChannel := make(chan any)
	resultChannels := make([]chan int, 0, len(l.clients))
	scenario := scenarios.Scenarios[l.config.Scenario]
	scenario.PrepareScenario(l.config, l.pbftConfig)
	time.Sleep(time.Second)

	go scenario.Run(stopChannel)
	for _, c := range l.clients {
		resultCh := make(chan int)
		resultChannels = append(resultChannels, resultCh)
		go func(stopCh chan any, resultCh chan int) {
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
		}(stopChannel, resultCh)
	}
	startTime := time.Now()
	time.Sleep(time.Duration(l.config.DurationSeconds) * time.Second)
	close(stopChannel)
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
	enableCh := make(chan any)
	disableCh := make(chan any)
	service := pbft.NewService(inputCh, requestCh, enableCh, disableCh, config)
	sender := pbft.NewSender(config)
	node := pbft.NewNode(config, sender, inputCh, requestCh, enableCh, disableCh)

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
