package pbft

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/Arman17Babaei/pbft/grpc/proto"
	"github.com/google/uuid"
)

const SEND_TIMEOUT = 400 * time.Millisecond
const HEARTBEAT_INTERVAL = 600 * time.Millisecond

var ELECTION_TIMEOUT = 1500*time.Millisecond + (time.Duration(rand.Int()%500) * time.Millisecond)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type Request struct {
	Command  string
	Callback chan bool
}

type Node struct {
	Mu          sync.Mutex
	Id          int            // Unique ID for the node
	Role        Role           // Current role: Follower, Candidate, or Leader
	CurrentTerm int            // Latest term seen
	VotedFor    *int           // Candidate ID voted for in the current term
	Log         []*pb.LogEntry // Log entries
	CommitIndex int            // Index of the highest log entry known to be committed
	LastApplied int            // Index of the highest log entry applied to the state machine
	NextIndex   map[int]int    // For Leader: next log index to send to each follower
	MatchIndex  map[int]int    // For Leader: highest log entry index known to be replicated on each follower

	Peers       []int // IDs of other nodes in the cluster
	Votes       int
	HeartbeatCh chan bool
	LeaderId    *int

	RequestCh chan Request
	CommandCh chan string
	PleaseCh  chan *pb.RequestPleaseRequest
	Callbacks map[string]chan bool
}

func NewNode(id int, others []int, requestCh chan Request, commandCh chan string, pleaseCh chan *pb.RequestPleaseRequest) *Node {
	node := &Node{
		Id:          id,
		Role:        Follower,
		CurrentTerm: 0,
		VotedFor:    nil,
		Log:         make([]*pb.LogEntry, 0),
		CommitIndex: -1,
		LastApplied: -1,
		NextIndex:   make(map[int]int),
		MatchIndex:  make(map[int]int),

		Peers:       others,
		Votes:       0,
		HeartbeatCh: make(chan bool),
		LeaderId:    nil,

		RequestCh: requestCh,
		CommandCh: commandCh,
		PleaseCh:  pleaseCh,
		Callbacks: make(map[string]chan bool, 0),
	}
	for _, peer := range others {
		node.NextIndex[peer] = 0
		node.MatchIndex[peer] = -1
	}
	go node.listenForRequests()
	go node.waitForElection()
	return node
}

func (n *Node) StartElection() {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	log.Printf("[Node %d] Starting election for term %d", n.Id, n.CurrentTerm+1)
	n.Role = Candidate
	n.CurrentTerm++
	n.VotedFor = &n.Id
	n.Votes = 1 // Vote for self

	for _, peer := range n.Peers {
		go n.sendRequestVote(peer)
	}
}

func (n *Node) getLastLogTerm() int {
	if len(n.Log) == 0 {
		return 0
	}
	return int(n.Log[len(n.Log)-1].Term)
}

func (n *Node) sendRequestVote(peerID int) {
	// Prepare and send a RequestVote RPC
	request := &pb.RequestVoteRequest{
		Term:         int32(n.CurrentTerm),
		CandidateID:  int32(n.Id),
		LastLogIndex: int32(len(n.Log) - 1),
		LastLogTerm:  int32(n.getLastLogTerm()),
	}
	response, err := SendRPCToPeer(peerID, "RequestVote", request) // Implement sendRPCToPeer
	if err != nil {
		// log.Printf("[Node %d] Failed to contact peer %d: %v", n.Id, peerID, err)
		return
	}

	// Process the vote response
	// log.Printf("[Node %d] Received vote response from %d: %+v", n.Id, peerID, response)
	n.processVoteResponse(response.(*pb.RequestVoteResponse))
}

func (n *Node) processVoteResponse(response *pb.RequestVoteResponse) {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	if int(response.Term) > n.CurrentTerm {
		n.LeaderId = nil
		n.becomeFollower(int(response.Term))
		return
	}

	if response.VoteGranted {
		n.Votes++
		if n.Votes >= len(n.Peers)/2 {
			n.becomeLeader()
		}
	}

	log.Printf("[Node %d] Votes: %d", n.Id, n.Votes)
}

func (n *Node) becomeFollower(term int) {
	log.Printf("[Node %d] Becoming follower for term %d", n.Id, term)
	n.Role = Follower
	n.CurrentTerm = term
	n.VotedFor = nil
}

func (n *Node) becomeLeader() {
	if n.Role == Leader {
		return
	}

	log.Printf("[Node %d] Becoming leader for term %d", n.Id, n.CurrentTerm)
	n.Role = Leader
	n.LeaderId = &n.Id
	n.startHeartbeat()
}

func (n *Node) waitForElection() {
	for {
		select {
		case <-time.After(ELECTION_TIMEOUT):
			if n.Role != Leader {
				n.StartElection()
			}
		case <-n.HeartbeatCh:
			n.becomeFollower(n.CurrentTerm)
		}
	}
}

func (n *Node) startHeartbeat() {
	ticker := time.NewTicker(HEARTBEAT_INTERVAL)
	go func() {
		for range ticker.C {
			if n.Role != Leader {
				ticker.Stop()
				return
			}
			n.broadcastAppendEntries()
		}
	}()
}

func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.Peers {
		go n.sendAppendEntries(peer)
	}
}

func (n *Node) sendAppendEntries(peerID int) {
	n.Mu.Lock()

	// Prepare AppendEntries RPC
	prevLogIndex := n.NextIndex[peerID] - 1
	// log.Printf("[Node %d] Sending logs: %v", n.Id, n.Log)
	request := &pb.AppendEntriesRequest{
		Term:         int32(n.CurrentTerm),
		LeaderID:     int32(n.Id),
		PrevLogIndex: int32(prevLogIndex),
		PrevLogTerm:  int32(n.getLogTerm(prevLogIndex)),
		Entries:      n.Log[n.NextIndex[peerID]:],
		LeaderCommit: int32(n.CommitIndex),
	}

	n.Mu.Unlock()

	response, err := SendRPCToPeer(peerID, "AppendEntries", request) // Implement sendRPCToPeer
	if err != nil {
		// log.Printf("[Node %d] Failed to send AppendEntries to peer %d: %v", n.Id, peerID, err)
		return
	}

	n.processAppendEntriesResponse(peerID, response.(*pb.AppendEntriesResponse))
}

func (n *Node) processAppendEntriesResponse(peerID int, response *pb.AppendEntriesResponse) {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	// log.Printf("[Node %d] Received AppendEntries response from %d: %+v", n.Id, peerID, response)

	if int(response.Term) > n.CurrentTerm {
		n.becomeFollower(int(response.Term))
		return
	}

	if response.Success {
		// Update nextIndex and matchIndex for the follower
		n.NextIndex[peerID] = len(n.Log)
		n.MatchIndex[peerID] = len(n.Log) - 1
		// log.Printf("[Node %d] Updated nextIndex and matchIndex for %d: %d %d", n.Id, peerID, n.NextIndex[peerID], n.MatchIndex[peerID])

		// Update commitIndex if a majority agrees
		n.updateCommitIndex()
	} else {
		// Decrement nextIndex and retry
		n.NextIndex[peerID]--
	}
}

func (n *Node) updateCommitIndex() {
	matchIndices := make([]int, len(n.Peers))
	for i, peer := range n.Peers {
		matchIndices[i] = n.MatchIndex[peer]
	}
	matchIndices = append(matchIndices, len(n.Log)-1) // Include leader's matchIndex
	sort.Ints(matchIndices)

	// Commit the log entry at the median index
	majorityIndex := matchIndices[len(matchIndices)/2]
	if majorityIndex > n.CommitIndex && int(n.Log[majorityIndex].Term) == n.CurrentTerm {
		// Call the callback for each committed log entry
		for i := n.CommitIndex + 1; i <= majorityIndex; i++ {
			log.Printf("[Node %d] Applying command: %v (UUID: %s)", n.Id, n.Log[i].Command, n.Log[i].Uuid)
			n.CommandCh <- n.Log[i].Command
			if n.Callbacks[n.Log[i].Uuid] != nil {
				n.Callbacks[n.Log[i].Uuid] <- true
			}
		}
		n.CommitIndex = majorityIndex
		// log.Printf("[Node %d] Updated commitIndex to %d", n.Id, n.CommitIndex)
	}
}

func (n *Node) getLogTerm(index int) int {
	if index < 0 || index >= len(n.Log) {
		return 0 // Default term
	}
	return int(n.Log[index].Term)
}

func (n *Node) listenForRequests() {
	for {
		select {
		case command := <-n.RequestCh:
			// log.Printf("[Node %d] Received command: %v", n.Id, command)
			err := n.applyCommandRequest(command, "")
			if err != nil {
				log.Printf("[Node %d] Failed to apply command: %v", n.Id, err)
			}
		case request := <-n.PleaseCh:
			// log.Printf("[Node %d] Received request: %v", n.Id, request)
			err := n.applyCommandRequest(Request{Command: request.Command}, request.Uuid)
			if err != nil {
				log.Printf("[Node %d] Failed to apply command: %v", n.Id, err)
			}
		}
	}
}

func (n *Node) applyCommandRequest(command Request, rUuid string) error {
	if rUuid == "" {
		rUuid = uuid.New().String()
	}
	n.Callbacks[rUuid] = command.Callback

	log.Printf("[Node %d] Applying command request: %v (UUID: %s)", n.Id, command.Command, rUuid)

	if n.Role != Leader {
		if n.LeaderId != nil {
			res, err := SendRPCToPeer(*n.LeaderId, "PleaseDoThis", &pb.RequestPleaseRequest{Command: command.Command, Uuid: rUuid})
			if err != nil {
				return fmt.Errorf("failed to send command to leader: %v", err)
			}
			if res.(*pb.RequestPleaseResponse).Success {
				return nil
			} else {
				return fmt.Errorf("leader failed to apply command")
			}
		}
		return fmt.Errorf("Node %d doesn't know the leader", n.Id)
	}

	n.Mu.Lock()
	defer n.Mu.Unlock()

	// Append the command to the local log
	entry := pb.LogEntry{
		Term:    int32(n.CurrentTerm),
		Command: command.Command,
		Uuid:    rUuid,
	}
	n.Log = append(n.Log, &entry)

	// Update matchIndex and nextIndex for log replication
	for _, peer := range n.Peers {
		go n.sendAppendEntries(peer)
	}

	return nil
}

func (n *Node) AppendEntriesHandler(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	response := &pb.AppendEntriesResponse{
		Term:    int32(n.CurrentTerm),
		Success: false,
	}

	// Update term if necessary
	if int(req.Term) > n.CurrentTerm {
		n.LeaderId = new(int)
		*n.LeaderId = int(req.LeaderID)
		n.becomeFollower(int(req.Term))
	}

	// Reject if term is smaller
	if int(req.Term) < n.CurrentTerm {
		// log.Printf("[Node %d] Rejecting AppendEntries from %d: term is smaller", n.Id, int(req.LeaderID))
		return response, nil
	}

	n.LeaderId = new(int)
	*n.LeaderId = int(req.LeaderID)

	// Reset election timer (heartbeat)
	n.resetElectionTimeout()

	// Check log consistency
	if int(req.PrevLogIndex) >= len(n.Log) ||
		(int(req.PrevLogIndex) >= 0 && n.Log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		// log.Printf("[Node %d] Rejecting AppendEntries from %d: log inconsistency", n.Id, int(req.LeaderID))
		return response, nil
	}

	// Append new entries
	for i, entry := range req.Entries {
		index := int(req.PrevLogIndex) + 1 + i
		if index < len(n.Log) {
			if n.Log[index].Term != entry.Term {
				// Conflict: remove entries starting from index
				n.Log = n.Log[:index]
			}
		}
		if index >= len(n.Log) {
			n.Log = append(n.Log, entry)
		}
	}

	// Update commit index
	// log.Printf("[Node %d] Leader commitIndex: %d, Node commitIndex: %d", n.Id, int(req.LeaderCommit), n.CommitIndex)
	if int(req.LeaderCommit) > n.CommitIndex {
		prevCommitIndex := n.CommitIndex
		n.CommitIndex = min(int(req.LeaderCommit), len(n.Log)-1)
		for i := prevCommitIndex + 1; i <= n.CommitIndex; i++ {
			log.Printf("[Node %d] Committing command: %v (UUID: %s)", n.Id, n.Log[i].Command, n.Log[i].Uuid)
			n.CommandCh <- n.Log[i].Command
			if n.Callbacks[n.Log[i].Uuid] != nil {
				n.Callbacks[n.Log[i].Uuid] <- true
			}
		}
	}

	response.Success = true
	// log.Printf("[Node %d] Appended entries from %d: log=%v", n.Id, int(req.LeaderID), n.Log)
	return response, nil
}

func (n *Node) RequestVoteHandler(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	response := &pb.RequestVoteResponse{
		Term:        int32(n.CurrentTerm),
		VoteGranted: false,
	}

	// Update term if necessary
	if int(req.Term) > n.CurrentTerm {
		n.LeaderId = nil
		n.becomeFollower(int(req.Term))
	}

	// Reject vote if term is stale
	if int(req.Term) < n.CurrentTerm {
		// log.Printf("[Node %d] Rejecting vote for %d: term is stale", n.Id, int(req.CandidateID))
		return response, nil
	}

	// Check if node already voted for another candidate
	if n.VotedFor != nil && *n.VotedFor != int(req.CandidateID) {
		return response, nil
	}

	lastLogIndex := len(n.Log) - 1
	lastLogTerm := n.getLastLogTerm()

	if int(req.LastLogTerm) < lastLogTerm ||
		(int(req.LastLogTerm) == lastLogTerm && int(req.LastLogIndex) < lastLogIndex) {
		// log.Printf("[Node %d] Rejecting vote for %d: log is stale", n.Id, int(req.CandidateID))
		return response, nil
	}

	// Grant vote
	n.VotedFor = new(int)
	*n.VotedFor = int(req.CandidateID)
	n.LeaderId = n.VotedFor
	n.resetElectionTimeout()

	response.VoteGranted = true
	// log.Printf("[Node %d] Voted for %d", n.Id, int(req.CandidateID))
	return response, nil
}

func (n *Node) resetElectionTimeout() {
	select {
	case n.HeartbeatCh <- true:
		// log.Printf("[Node %d] Reset election timeout", n.Id)
	default:
		// log.Printf("[Node %d] Election timeout channel is full", n.Id)
	}
}
