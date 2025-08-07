package load_tester

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/Arman17Babaei/pbft/client"
	loader "github.com/Arman17Babaei/pbft/config"
	loadtestconfig "github.com/Arman17Babaei/pbft/load_tester/configs"
	"github.com/Arman17Babaei/pbft/load_tester/monitoring"
	"github.com/Arman17Babaei/pbft/load_tester/scenarios"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
)

type LoadTest struct {
	config     *loadtestconfig.Config
	pbftConfig *configs.Config
	clients    []*client.Client
}

type ClientResult struct {
	Sent   int
	Done   int
	Failed int
}

func (r *ClientResult) Add(other ClientResult) {
	r.Sent += other.Sent
	r.Done += other.Done
	r.Failed += other.Failed
}

func NewLoadTest(c *loadtestconfig.Config) *LoadTest {
	l := &LoadTest{config: c}
	var pbftConfig configs.Config
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
		configCopy.GrpcAddress.Port = 3000 + i + 1
		configCopy.HttpAddress = nil
		l.clients = append(l.clients, startClient(&configCopy))
	}

	return l
}

func (l *LoadTest) Run() {
	intervalNs := int64(l.config.NumClients) * time.Second.Nanoseconds() / int64(l.config.Throughput)
	intervalDuration := time.Duration(intervalNs) * time.Nanosecond
	stopChannel := make(chan any)
	resultChannels := make([]chan ClientResult, 0, len(l.clients))
	scenario := scenarios.Scenarios[l.config.Scenario]
	scenario.PrepareScenario(l.config, l.pbftConfig)
	time.Sleep(time.Second)

	go scenario.Run(stopChannel)

	for i, c := range l.clients {
		resultCh := make(chan ClientResult)
		resultChannels = append(resultChannels, resultCh)
		go func(ind int, c *client.Client, stopCh chan any, resultCh chan ClientResult) {
			ticker := time.NewTicker(intervalDuration)
			responseCh := make(chan *pb.OperationResult, 10_000)
			failed := 0
			sent := 0
			done := 0
			for {
				select {
				case <-ticker.C:
					op := &pb.Operation{Type: pb.Operation_GET}
					err := c.SendRequest(op, responseCh)
					if err != nil {
						monitoring.RequestCounter.WithLabelValues(strconv.Itoa(ind), err.Error()).Inc()
						failed++
					} else {
						monitoring.RequestCounter.WithLabelValues(strconv.Itoa(ind), "sent").Inc()
						sent++
					}
				case <-responseCh:
					monitoring.RequestCounter.WithLabelValues(strconv.Itoa(ind), "repliedTo").Inc()
					done++
				case <-stopCh:
					resultCh <- ClientResult{Sent: sent, Done: done, Failed: failed}
					break
				}
			}
		}(i, c, stopChannel, resultCh)
	}
	startTime := time.Now()
	time.Sleep(time.Duration(l.config.DurationSeconds) * time.Second)
	close(stopChannel)
	totalResult := ClientResult{}
	for _, ch := range resultChannels {
		result := <-ch
		totalResult.Add(result)
	}

	fmt.Printf("Total sent: %d\n", totalResult.Sent)
	fmt.Printf("Total failed: %d\n", totalResult.Failed)
	fmt.Printf("Total done: %d\n", totalResult.Done)
	fmt.Printf("Throughput: %f\n", float64(totalResult.Done)/time.Since(startTime).Seconds())
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
	go c.Serve()
	return c
}
