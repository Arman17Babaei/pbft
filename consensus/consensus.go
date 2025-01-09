package consensus

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Arman17Babaei/pbft/grpc"
	pb "github.com/Arman17Babaei/pbft/grpc/proto"
	"github.com/Arman17Babaei/pbft/pbft"
)

type ConsensusManager struct {
	mu          sync.Mutex
	value       int
	status      string
	myPort      int
	otherPorts  []int
	pbftStarted bool
	pbftCluster *PbftCluster
	requestCh   chan pbft.Request
	commandCh   chan string
}

type Command struct {
	Value     int    `json:"value"`
	Operation string `json:"operation"`
}

type PbftCluster struct {
	Node        *pbft.Node
	PbftService *grpc.PbftService
}

func NewConsensusManager(initValue int, myPort int, otherPorts []int) *ConsensusManager {
	return &ConsensusManager{
		value:      initValue,
		status:     "Initialized",
		myPort:     myPort,
		otherPorts: otherPorts,
	}
}

func (cm *ConsensusManager) Start() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.pbftStarted {
		fmt.Println("Pbft already started")
		return
	}
	fmt.Printf("Starting Pbft on port %d with peers %v...\n", cm.myPort, cm.otherPorts)

	cm.pbftStarted = true
	cm.requestCh = make(chan pbft.Request, 20)
	cm.commandCh = make(chan string)
	pleaseCh := make(chan *pb.RequestPleaseRequest, 20)
	node := pbft.NewNode(cm.myPort, cm.otherPorts, cm.requestCh, cm.commandCh, pleaseCh)
	cm.pbftCluster = &PbftCluster{
		Node:        node,
		PbftService: grpc.NewPbftService(node, cm.myPort, pleaseCh),
	}
	cm.pbftCluster.Node.StartElection()
	go cm.listenForCommands()
	cm.status = "Pbft Started"
}

func (cm *ConsensusManager) Get() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.value
}

func (cm *ConsensusManager) Add(value int) error {
	return cm.submitCommand(Command{Operation: "add", Value: value})
}

func (cm *ConsensusManager) Sub(value int) error {
	return cm.submitCommand(Command{Operation: "sub", Value: value})
}

func (cm *ConsensusManager) Mul(value int) error {
	return cm.submitCommand(Command{Operation: "mul", Value: value})
}

func (cm *ConsensusManager) GetStatus() string {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.status
}

func (cm *ConsensusManager) submitCommand(command Command) error {
	commandS, err := json.Marshal(command)
	if err != nil {
		fmt.Printf("Failed to marshal command: %v\n", err)
		return err
	}
	cb := make(chan bool)
	cm.requestCh <- pbft.Request{Command: string(commandS), Callback: cb}
	<-cb
	return nil
}

func (cm *ConsensusManager) listenForCommands() {
	for command := range cm.commandCh {
		fmt.Printf("Received command: %s\n", command)
		cm.applyCommand(command)
		fmt.Printf("Current value: %d\n", cm.value)
	}
}

func (cm *ConsensusManager) applyCommand(command string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c := Command{}
	err := json.Unmarshal([]byte(command), &c)
	if err != nil {
		fmt.Printf("Failed to unmarshal command: %v\n", err)
		return
	}

	fmt.Printf("Applying command: %s %d\n", c.Operation, c.Value)

	switch c.Operation {
	case "add":
		cm.value += c.Value
	case "sub":
		cm.value -= c.Value
	case "mul":
		cm.value *= c.Value
	}

	cm.status = fmt.Sprintf("Applied command: %s %d", c.Operation, c.Value)
	fmt.Println("Command committed.")
}
