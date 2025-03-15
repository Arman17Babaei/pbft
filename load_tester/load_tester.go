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
	"os"
	"os/exec"
	"time"
)

type LoadTest struct {
	config     *loadtestconfig.Config
	pbftConfig *pbft.Config
	clients    []*client.Client
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
	for id := range pbftConfig.PeersAddress {
		log.WithField("node", id).Info("node configuration")
		err := startNode(id)
		if err != nil {
			log.WithError(err).Fatal("could not start node")
		}
	}
	time.Sleep(5 * time.Second)

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

func startNode(id string) error {
	cmd := exec.Command(
		"/home/arman-babaei/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.23.4.linux-amd64/bin/go",
		"run", "/home/arman-babaei/sharif/distributed/pbft/cmd/node/main.go",
		"--id", id)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Start()
}

func startClient(config *client.Config) *client.Client {
	c := client.NewClient(config)
	httpServer := client.NewHttpServer(config, c)
	go c.Serve()
	go httpServer.Serve()
	return c
}
