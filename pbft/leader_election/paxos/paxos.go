package paxos

import (
	"fmt"
	"sync"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// Node interface defines the methods that Paxos election needs from the main node
type Node interface {
	GetCurrentView() int64
	GetCurrentViewLeader() string
}

// PaxosElection implements the Paxos consensus algorithm for leader election
// It manages the Prepare, Accept, and Learn phases of the Paxos protocol
type PaxosElection struct {
	mu sync.RWMutex

	config *configs.Config
	node   Node
	sender *Sender

	// Paxos state variables
	currentView        int64
	currentViewLeader  string
	currentTerm        int64
	maxProposalId      int64
	proposalId         int64
	proposedValue      string
	acceptedProposalId int64
	acceptedValue      string

	// Election state
	NewLeader   string
	isLeader    bool
	isCandidate bool
	NewLeaderCh chan string

	// Message handling
	leaderElectionCh chan struct{}
	paxosCh          <-chan proto.Message
	stopCh           chan struct{}

	// Timeout control
	electionTimeout time.Duration
	electionTimer   *time.Timer

	// Response tracking
	prepareRequests map[int64]map[string]*pb.PaxosPrepareRequest
	acceptRequests  map[int64]map[string]*pb.PaxosAcceptRequest
}

// NewPaxosElection creates a new Paxos election instance
func NewPaxosElection(config *configs.Config, node Node, sender *Sender) *PaxosElection {
	electionTimeout := time.Duration(config.Timers.ViewChangeTimeoutMs) * time.Millisecond

	return &PaxosElection{
		config: config,
		node:   node,
		sender: sender,

		currentView:        node.GetCurrentView(),
		currentViewLeader:  node.GetCurrentViewLeader(),
		currentTerm:        0,
		maxProposalId:      0,
		proposalId:         0,
		proposedValue:      "",
		acceptedValue:      "",
		acceptedProposalId: 0,

		NewLeader:        "",
		isLeader:         false,
		isCandidate:      false,
		leaderElectionCh: make(chan string, 10),
		stopCh:           make(chan struct{}),

		electionTimeout: electionTimeout,
		electionTimer:   time.NewTimer(electionTimeout),

		prepareRequests: make(map[int64]map[string]*pb.PaxosPrepareRequest),
		acceptRequests:  make(map[int64]map[string]*pb.PaxosAcceptRequest),
	}
}

func (p *PaxosElection) UpdatePaxosElection(node Node) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.currentView = node.GetCurrentView()
	p.currentViewLeader = node.GetCurrentViewLeader()
}

// SetMessageChannel sets the channel for receiving Paxos messages
func (p *PaxosElection) SetMessageChannel(paxosCh <-chan proto.Message) {
	p.paxosCh = paxosCh
}

// Start begins the Paxos election process
func (p *PaxosElection) Start() error {
	log.WithField("node-id", p.config.Id).Info("Starting Paxos election")

	// Start message handler loop
	go p.runMessageHandler()

	// Start election timeout handler
	go p.runElectionManager()

	return nil
}

// Stop halts the Paxos election process
func (p *PaxosElection) Stop() error {
	log.WithField("node-id", p.config.Id).Info("Stopping Paxos election")
	close(p.stopCh)
	return nil
}

// runMessageHandler processes incoming Paxos messages
func (p *PaxosElection) runMessageHandler() {
	for {
		select {
		case msg := <-p.paxosCh:
			p.handleMessage(msg)
		case <-p.stopCh:
			return
		}
	}
}

// handleMessage routes different types of Paxos messages to appropriate handlers
func (p *PaxosElection) handleMessage(msg proto.Message) {
	timer := monitoring.ResponseTimeSummary.WithLabelValues(p.config.Id, "paxos-message")
	defer timer.ObserveDuration()

	switch m := msg.(type) {
	case *pb.PaxosPrepareRequest:
		p.handlePrepareRequest(m)
	case *pb.PaxosPrepareResponse:
		p.handlePrepareResponse(m)
	case *pb.PaxosAcceptRequest:
		p.handleAcceptRequest(m)
	case *pb.PaxosAcceptResponse:
		p.handleAcceptResponse(m)
	case *pb.PaxosLearnRequest:
		p.handleLearnRequest(m)
	case *pb.ElectionStatusRequest:
		p.handleStatusRequest(m)
	default:
		log.WithField("message-type", fmt.Sprintf("%T", msg)).Warn("Unknown paxos message type")
	}
}

// handlePrepareRequest processes incoming Prepare phase requests
func (p *PaxosElection) handlePrepareRequest(req *pb.PaxosPrepareRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Debug("Handling prepare request")

	// Update term if request has higher term
	if req.Term > p.currentTerm {
		p.currentTerm = req.Term
		p.isLeader = false
	}

	// Accept prepare if proposal ID is higher
	if req.ProposalId > p.acceptedProposalId {
		p.acceptedProposalId = req.ProposalId

		// Send prepare response
		response := &pb.PaxosPrepareResponse{
			Term:               p.currentTerm,
			Accepted:           true,
			AcceptedProposalId: p.acceptedProposalId,
			AcceptedValue:      p.acceptedValue,
			VoterId:            p.config.Id,
		}

		p.sender.SendRPCToPeer(req.ProposerId, "PaxosPrepare", response)
	} else {
		// Reject prepare
		response := &pb.PaxosPrepareResponse{
			Term:     p.currentTerm,
			Accepted: false,
			VoterId:  p.config.Id,
		}

		p.sender.SendRPCToPeer(req.ProposerId, "PaxosPrepare", response)
	}
}

// handlePrepareResponse processes Prepare phase responses
func (p *PaxosElection) handlePrepareResponse(resp *pb.PaxosPromiseRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	proposalId := p.proposalId
	if p.prepareResponses[proposalId] == nil {
		p.prepareResponses[proposalId] = make(map[string]*pb.PaxosPrepareResponse)
	}

	p.prepareResponses[proposalId][resp.VoterId] = resp

	// Check if we have majority support
	if len(p.prepareResponses[proposalId]) >= p.getMajority() {
		acceptedCount := 0
		var highestAcceptedValue string
		var highestAcceptedProposalId int64

		for _, response := range p.prepareResponses[proposalId] {
			if response.Accepted {
				acceptedCount++
				if response.AcceptedProposalId > highestAcceptedProposalId {
					highestAcceptedProposalId = response.AcceptedProposalId
					highestAcceptedValue = response.AcceptedValue
				}
			}
		}

		if acceptedCount >= p.getMajority() {
			// Proceed to Accept phase
			p.startAcceptPhase(proposalId, highestAcceptedValue)
		}
	}
}

// handleAcceptRequest processes incoming Accept phase requests
func (p *PaxosElection) handleAcceptRequest(req *pb.PaxosAcceptRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Debug("Handling accept request")

	// Update term if request has higher term
	if req.Term > p.currentTerm {
		p.currentTerm = req.Term
		p.isLeader = false
	}

	// Accept if proposal ID >= accepted proposal ID
	if req.ProposalId >= p.acceptedProposalId {
		p.acceptedProposalId = req.ProposalId
		p.acceptedValue = req.LeaderId

		// Send accept response
		response := &pb.PaxosAcceptResponse{
			Term:     p.currentTerm,
			Accepted: true,
			VoterId:  p.config.Id,
		}

		p.sender.SendRPCToPeer(req.ProposerId, "PaxosAccept", response)
	} else {
		// Reject accept
		response := &pb.PaxosAcceptResponse{
			Term:     p.currentTerm,
			Accepted: false,
			VoterId:  p.config.Id,
		}

		p.sender.SendRPCToPeer(req.ProposerId, "PaxosAccept", response)
	}
}

// handleAcceptResponse processes Accept phase responses
func (p *PaxosElection) handleAcceptResponse(resp *pb.PaxosAcceptResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()

	proposalId := p.proposalId
	if p.acceptResponses[proposalId] == nil {
		p.acceptResponses[proposalId] = make(map[string]*pb.PaxosAcceptResponse)
	}

	p.acceptResponses[proposalId][resp.VoterId] = resp

	// Check if we have majority support
	if len(p.acceptResponses[proposalId]) >= p.getMajority() {
		acceptedCount := 0

		for _, response := range p.acceptResponses[proposalId] {
			if response.Accepted {
				acceptedCount++
			}
		}

		if acceptedCount >= p.getMajority() {
			// Proceed to Learn phase
			p.startLearnPhase(proposalId)
		}
	}
}

// handleStatusRequest processes election status requests
func (p *PaxosElection) handleStatusRequest(req *pb.ElectionStatusRequest) {
	response := &pb.ElectionStatusResponse{
		CurrentTerm:   p.currentTerm,
		CurrentLeader: p.currentLeader,
		NodeState:     p.getNodeState(),
		ViewId:        p.currentTerm, // Use term as view ID
		LastHeartbeat: time.Now().Unix(),
	}

	p.sender.SendRPCToPeer(req.NodeId, "GetElectionStatus", response)
}

// startElection initiates a new election round
func (p *PaxosElection) startElection() {
	p.mu.Lock()
	p.currentTerm++
	p.proposalId = p.generateProposalId()
	p.isLeader = false
	p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", p.currentTerm).
		WithField("proposal-id", p.proposalId).
		Info("Starting new election")

	// Send Prepare requests
	p.startPreparePhase(p.proposalId)
}

// startPreparePhase initiates the Prepare phase
func (p *PaxosElection) startPreparePhase(proposalId int64) {
	request := &pb.PaxosPrepareRequest{
		Term:                   p.currentTerm,
		ProposalId:             proposalId,
		ProposerId:             p.config.Id,
		ViewId:                 p.currentTerm,
		LastAcceptedProposalId: p.acceptedProposalId,
		LastAcceptedValue:      p.acceptedValue,
	}

	p.sender.Broadcast("PaxosPrepare", request)
}

// startAcceptPhase initiates the Accept phase
func (p *PaxosElection) startAcceptPhase(proposalId int64, value string) {
	request := &pb.PaxosAcceptRequest{
		Term:       p.currentTerm,
		ProposalId: proposalId,
		ProposerId: p.config.Id,
		ViewId:     p.currentTerm,
		LeaderId:   value,
	}

	p.sender.Broadcast("PaxosAccept", request)
}

// startLearnPhase initiates the Learn phase
func (p *PaxosElection) startLearnPhase(proposalId int64) {
	request := &pb.PaxosLearnRequest{
		Term:       p.currentTerm,
		ProposalId: proposalId,
		ProposerId: p.config.Id,
		ViewId:     p.currentTerm,
		LeaderId:   p.acceptedValue,
	}

	p.sender.Broadcast("PaxosLearn", request)
}

// runElectionTimer handles election timeouts
func (p *PaxosElection) runElectionManager() {
	for {
		select {
		case <-p.NewLeaderCh:
			p.startElection()
		case <-p.electionTimer.C:
			p.startElection()
			p.electionTimer.Reset(p.electionTimeout)
		case <-p.stopCh:
			return
		}
	}
}

// generateProposalId creates a unique proposal ID
func (p *PaxosElection) generateProposalId() int64 {
	return time.Now().UnixNano() + int64(len(p.config.PeersAddress))
}

// getMajority returns the number of nodes needed for majority
func (p *PaxosElection) getMajority() int {
	return len(p.config.PeersAddress)/2 + 1
}

// getNodeState returns the current state of this node
func (p *PaxosElection) getNodeState() string {
	if p.isLeader {
		return "leader"
	}
	return "follower"
}

func (p *PaxosElection) FindLeaderForView(viewId int64, callbackCh chan string) {
	// If we already know the leader for this view, return immediately
	if p.currentViewLeader != "" && p.currentView == viewId {
		callbackCh <- p.currentViewLeader
		return
	}

	// Otherwise start a new election
	go func() {
		p.startElection()
		// Wait for election result
		select {
		case leader := <-p.NewLeaderCh:
			callbackCh <- leader
		case <-time.After(p.electionTimeout):
			callbackCh <- ""
		}
	}()
}

func (p *PaxosElection) GetCurrentLeader() string {
	return p.currentViewLeader
}

func (p *PaxosElection) IsLeader() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isLeader
}

func (p *PaxosElection) HandleMessage(msg proto.Message) error {
	// This method is called by external components to handle messages
	p.handleMessage(msg)
	return nil
}
