package pbft

//go:generate mockgen -source=node.go -destination=node_mock.go -package=pbft

import (
	"maps"
	"math"
	"slices"
	"sync"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/leader_election"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ISender interface {
	SendRPCToClient(clientAddress, method string, message proto.Message)
	SendRPCToPeer(peerID string, method string, message proto.Message)
	Broadcast(method string, message proto.Message)
}

type ViewChanger interface {
	RequestExecuted(viewId int64)
}

type LeaderElection interface {
	GetLeader(view int64) string
}

type ViewData struct {
	LeaderId       string
	CurrentView    int64
	IsInViewChange bool

	InProgressRequests map[int64]any
	LastSequenceNumber int64

	Preprepares    map[int64]*pb.PrePrepareRequest
	Prepares       map[int64]map[string]*pb.PrepareRequest
	FailedPrepares map[int64]map[string]*pb.PrepareRequest
	Commits        map[int64]map[string]*pb.CommitRequest
	FailedCommits  map[int64]map[string]*pb.CommitRequest
}

type Node struct {
	mu        sync.RWMutex
	config    *configs.Config
	sender    ISender
	Store     *Store
	InputCh   <-chan proto.Message
	RequestCh <-chan *pb.ClientRequest

	LeaderElection LeaderElection
	viewChanger    ViewChanger

	ViewData        *ViewData
	PendingRequests []*pb.ClientRequest

	Enabled   bool
	EnableCh  <-chan any
	DisableCh <-chan any
	StopCh    chan any
}

func NewNode(
	config *configs.Config,
	sender ISender,
	inputCh <-chan proto.Message,
	requestCh <-chan *pb.ClientRequest,
	enableCh <-chan any,
	disableCh <-chan any,
	store *Store,
) *Node {
	leaderElection := leader_election.NewRoundRobinLeaderElection(config)
	return &Node{
		config:    config,
		sender:    sender,
		Store:     store,
		InputCh:   inputCh,
		RequestCh: requestCh,

		LeaderElection: leaderElection,

		ViewData: NewViewData(0, 0, leaderElection.GetLeader(0)),

		PendingRequests: []*pb.ClientRequest{},

		Enabled:   config.General.EnabledByDefault,
		EnableCh:  enableCh,
		DisableCh: disableCh,
		StopCh:    make(chan any),
	}
}

func NewViewData(viewId, initialSequenceNumber int64, leaderId string) *ViewData {
	return &ViewData{
		IsInViewChange: false,

		Preprepares:    make(map[int64]*pb.PrePrepareRequest),
		Prepares:       make(map[int64]map[string]*pb.PrepareRequest),
		FailedPrepares: make(map[int64]map[string]*pb.PrepareRequest),
		Commits:        make(map[int64]map[string]*pb.CommitRequest),
		FailedCommits:  make(map[int64]map[string]*pb.CommitRequest),

		InProgressRequests: make(map[int64]any),

		CurrentView:        viewId,
		LeaderId:           leaderId,
		LastSequenceNumber: initialSequenceNumber,
	}
}

func (n *Node) SetViewChanger(viewChanger ViewChanger) {
	n.viewChanger = viewChanger
}

func (n *Node) Run() {
	n.sender.Broadcast("GetStatus", &pb.StatusRequest{ReplicaId: n.config.Id})
	for {
		if !n.Enabled {
			<-n.EnableCh
			n.Enabled = true
			exhaustChannel(n.DisableCh)
			n.sender.Broadcast("GetStatus", &pb.StatusRequest{ReplicaId: n.config.Id})
		}

		select {
		case request := <-n.RequestCh:
			if len(n.ViewData.InProgressRequests) >= n.config.General.MaxOutstandingRequests {
				monitoring.ClientRequestStatusCounter.WithLabelValues("dropped").Inc()
				continue
			}
			n.handleClientRequest(request)
		case input := <-n.InputCh:
			n.handleInput(input)
		case <-n.DisableCh:
			n.Enabled = false
			exhaustChannel(n.EnableCh)
		case <-n.StopCh:
			return
		}
	}
}

func exhaustChannel[T any](channel <-chan T) {
	for {
		select {
		case <-channel:
			continue
		default:
			return
		}
	}
}

func (n *Node) Stop() {
	close(n.StopCh)
}
func (n *Node) isPrimary() bool {
	return n.config.Id == n.ViewData.LeaderId
}

func (n *Node) handleInput(input proto.Message) {
	timer := prometheus.NewTimer(monitoring.ResponseTimeSummary.WithLabelValues(n.config.Id, string(input.ProtoReflect().Descriptor().Name())))
	defer timer.ObserveDuration()

	switch msg := input.(type) {
	case *pb.PiggyBackedPrePareRequest:
		n.handlePrePrepareRequest(msg)
	case *pb.PrepareRequest:
		n.handlePrepareRequest(msg)
	case *pb.CommitRequest:
		n.handleCommitRequest(msg)
	case *pb.CheckpointRequest:
		n.handleCheckpointRequest(msg)
	case *pb.StatusRequest:
		n.handleStatusRequest(msg)
	case *pb.StatusResponse:
		n.handleStatusResponse(msg)
	}
}

func (n *Node) handleClientRequest(msg *pb.ClientRequest) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	timer := prometheus.NewTimer(monitoring.ResponseTimeSummary.WithLabelValues(n.config.Id, "client-request"))
	defer timer.ObserveDuration()

	if !n.isPrimary() {
		log.WithField("request", msg.String()).Info("Received client request but not primary")
		log.WithField("my-id", n.config.Id).WithField("leader", n.ViewData.LeaderId).Info("Forwarding request to leader")
		n.sender.SendRPCToPeer(n.ViewData.LeaderId, "Request", msg)
		monitoring.ClientRequestStatusCounter.WithLabelValues("forward-to-leader").Inc()
		return
	}

	if n.ViewData.IsInViewChange {
		log.Warn("Dismissing request because in view change")
		monitoring.ClientRequestStatusCounter.WithLabelValues("in-view-change").Inc()
		return
	}

	log.WithField("request", msg.String()).Info("Received client request")

	if len(n.ViewData.InProgressRequests) >= n.config.General.MaxOutstandingRequests {
		log.Warn("Too many outstanding requests, putting request in pending queue")
		n.PendingRequests = append(n.PendingRequests, msg)
		monitoring.ClientRequestStatusCounter.WithLabelValues("too-many-outstanding").Inc()
		return
	}

	n.ViewData.LastSequenceNumber++
	n.ViewData.InProgressRequests[n.ViewData.LastSequenceNumber] = struct{}{}
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.ViewData.InProgressRequests)))

	prepreareMessage := &pb.PiggyBackedPrePareRequest{
		PrePrepareRequest: &pb.PrePrepareRequest{
			ViewId:         n.ViewData.CurrentView,
			SequenceNumber: n.ViewData.LastSequenceNumber,
		},
		Requests: []*pb.ClientRequest{msg},
	}
	n.ViewData.Preprepares[n.ViewData.LastSequenceNumber] = prepreareMessage.PrePrepareRequest

	n.Store.AddRequests(n.ViewData.LastSequenceNumber, []*pb.ClientRequest{msg})
	n.sender.Broadcast("PrePrepare", prepreareMessage)
	monitoring.ClientRequestStatusCounter.WithLabelValues("success").Inc()
}

func (n *Node) handlePrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	log.WithField("id", msg.PrePrepareRequest.SequenceNumber).WithField("my-id", n.config.Id).Info("PrePrepare received")
	if n.isPrimary() {
		log.WithField("request", msg.String()).WithField("my-id", n.config.Id).Warn("Received pre-prepare request but is primary")
		return
	}

	if n.ViewData.IsInViewChange {
		log.Warn("Dismissing preprepare because in view change")
		return
	}

	log.WithField("request", msg.String()).Info("Received pre-prepare request")

	if !n.verifyPrePrepareRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify pre-prepare request")
		return
	}

	sequenceNumber := msg.PrePrepareRequest.SequenceNumber
	n.ViewData.InProgressRequests[sequenceNumber] = struct{}{}
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.ViewData.InProgressRequests)))
	n.ViewData.Preprepares[sequenceNumber] = msg.PrePrepareRequest

	prepareMessage := &pb.PrepareRequest{
		ViewId:         msg.PrePrepareRequest.ViewId,
		SequenceNumber: sequenceNumber,
		RequestDigest:  msg.PrePrepareRequest.RequestDigest,
		ReplicaId:      n.config.Id,
	}

	n.ViewData.Prepares[sequenceNumber] = make(map[string]*pb.PrepareRequest)
	n.ViewData.Prepares[sequenceNumber][n.config.Id] = prepareMessage

	n.Store.AddRequests(msg.PrePrepareRequest.SequenceNumber, msg.Requests)
	n.sender.Broadcast("Prepare", prepareMessage)

	for _, prepare := range n.ViewData.FailedPrepares[sequenceNumber] {
		go n.handlePrepareRequest(prepare)
	}
	for _, commit := range n.ViewData.FailedCommits[sequenceNumber] {
		go n.handleCommitRequest(commit)
	}
}

func (n *Node) handlePrepareRequest(msg *pb.PrepareRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.WithField("request", msg.String()).Info("Received prepare request")

	if n.ViewData.IsInViewChange {
		log.Warn("Dismissing prepare because in view change")
		return
	}

	if !n.verifyPrepareRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify prepare request")

		if _, ok := n.ViewData.FailedPrepares[msg.SequenceNumber]; !ok {
			n.ViewData.FailedPrepares[msg.SequenceNumber] = make(map[string]*pb.PrepareRequest)
		}

		n.ViewData.FailedPrepares[msg.SequenceNumber][msg.ReplicaId] = msg
		return
	}

	sequenceNumber := msg.SequenceNumber

	if _, ok := n.ViewData.Prepares[sequenceNumber]; !ok {
		n.ViewData.Prepares[sequenceNumber] = make(map[string]*pb.PrepareRequest)
	}

	n.ViewData.Prepares[sequenceNumber][msg.ReplicaId] = msg

	prepareNodes := make([]string, 0)
	for id := range n.ViewData.Prepares[sequenceNumber] {
		prepareNodes = append(prepareNodes, id)
	}
	log.WithField("prepare-nodes", prepareNodes).Info("prepare nodes")

	// prepared
	if len(n.ViewData.Prepares[sequenceNumber]) == 2*n.config.F() {
		commitMessage := &pb.CommitRequest{
			ViewId:         msg.ViewId,
			SequenceNumber: msg.SequenceNumber,
			RequestDigest:  msg.RequestDigest,
			ReplicaId:      n.config.Id,
		}

		go n.handleCommitRequest(commitMessage)
		n.sender.Broadcast("Commit", commitMessage)
	}
}

func (n *Node) handleCommitRequest(msg *pb.CommitRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.WithField("request", msg.String()).Info("Received commit request")

	if n.ViewData.IsInViewChange {
		log.Warn("Dismissing commit because in view change")
		return
	}

	if !n.verifyCommitRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify commit request")

		if _, ok := n.ViewData.FailedCommits[msg.SequenceNumber]; !ok {
			n.ViewData.FailedCommits[msg.SequenceNumber] = make(map[string]*pb.CommitRequest)
		}

		n.ViewData.FailedCommits[msg.SequenceNumber][msg.ReplicaId] = msg
		return
	}

	sequenceNumber := msg.SequenceNumber
	if _, ok := n.ViewData.Commits[sequenceNumber]; !ok {
		n.ViewData.Commits[sequenceNumber] = make(map[string]*pb.CommitRequest)
	}

	n.ViewData.Commits[sequenceNumber][msg.ReplicaId] = msg

	committeds := make([]string, 0)
	for id := range n.ViewData.Commits[sequenceNumber] {
		committeds = append(committeds, id)
	}
	log.WithField("committed-nodes", committeds).Info("committed nodes")

	// committed
	if len(n.ViewData.Commits[sequenceNumber]) == 2*n.config.F()+1 {
		reqs, resps, checkpoints := n.Store.Commit(msg)

		delete(n.ViewData.InProgressRequests, sequenceNumber)
		monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.ViewData.InProgressRequests)))

		for _, checkpoint := range checkpoints {
			checkpoint.ViewId = n.ViewData.CurrentView
			go n.handleCheckpointRequest(checkpoint)
			n.sender.Broadcast("Checkpoint", checkpoint)
		}

		for i, req := range reqs {
			reply := &pb.ClientResponse{
				ViewId:      msg.ViewId,
				TimestampNs: req.TimestampNs,
				ClientId:    req.ClientId,
				ReplicaId:   n.config.Id,
				Result:      resps[i],
			}
			n.sender.SendRPCToClient(req.Callback, "Response", reply)
			n.viewChanger.RequestExecuted(msg.ViewId)
		}

		if len(n.ViewData.InProgressRequests) < n.config.General.MaxOutstandingRequests && len(n.PendingRequests) > 0 {
			pendings := n.PendingRequests
			n.PendingRequests = []*pb.ClientRequest{}
			for _, pending := range pendings {
				go n.handleClientRequest(pending)
			}
		}
	}
}

func (n *Node) handleCheckpointRequest(msg *pb.CheckpointRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.WithField("request", msg.String()).Info("Received checkpoint request")

	stableSequenceNumber := n.Store.AddCheckpointRequest(msg)
	if stableSequenceNumber == nil {
		return
	}

	for seqNo := range n.ViewData.Preprepares {
		if seqNo <= *stableSequenceNumber {
			delete(n.ViewData.Preprepares, seqNo)
		}
	}

	for seqNo := range n.ViewData.Prepares {
		if seqNo <= *stableSequenceNumber {
			delete(n.ViewData.Prepares, seqNo)
		}
	}

	for seqNo := range n.ViewData.Commits {
		if seqNo <= *stableSequenceNumber {
			delete(n.ViewData.Commits, seqNo)
		}
	}

	for req := range n.ViewData.InProgressRequests {
		if req <= *stableSequenceNumber {
			delete(n.ViewData.InProgressRequests, req)
		}
	}
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.ViewData.InProgressRequests)))
}

func (n *Node) GetCurrentPreparedRequests() []*pb.ViewChangePreparedMessage {
	n.mu.RLock()
	defer n.mu.RUnlock()

	prepreparedProof := make([]*pb.ViewChangePreparedMessage, 0, len(n.ViewData.Commits))
	for seqNo := range n.ViewData.Commits {
		prepreparedProof = append(prepreparedProof, &pb.ViewChangePreparedMessage{
			PrePrepareRequest: n.ViewData.Preprepares[seqNo],
			PreparedMessages:  slices.Collect(maps.Values(n.ViewData.Prepares[seqNo])),
		})
	}
	return prepreparedProof
}

func (n *Node) GoToViewChange() {
	n.ViewData.IsInViewChange = true
}

func (n *Node) HandleNewViewRequest(msg *pb.NewViewRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.WithField("my-id", n.config.Id).Info("Received new view request")

	if msg.NewViewId < n.ViewData.CurrentView {
		log.WithField("request", msg.String()).WithField("current-view", n.ViewData.CurrentView).Warn("Received new view request with old view")
		return
	}

	minSeqNo := int64(math.MaxInt64)
	for _, preprepare := range msg.Preprepares {
		minSeqNo = min(minSeqNo, preprepare.SequenceNumber)
	}

	n.ViewData = NewViewData(msg.NewViewId, minSeqNo, msg.ReplicaId)

	log.WithField("my-id", n.config.Id).WithField("leader-id", n.ViewData.LeaderId).Error("entered new view")
	for _, preprepare := range msg.Preprepares {
		n.ViewData.Preprepares[preprepare.SequenceNumber] = preprepare
		n.ViewData.Prepares[preprepare.SequenceNumber] = make(map[string]*pb.PrepareRequest)
		prepareMessage := &pb.PrepareRequest{
			ViewId:         preprepare.ViewId,
			SequenceNumber: preprepare.SequenceNumber,
			RequestDigest:  preprepare.RequestDigest,
			ReplicaId:      n.config.Id,
		}
		n.ViewData.Prepares[preprepare.SequenceNumber][n.config.Id] = prepareMessage
		n.sender.Broadcast("Prepare", prepareMessage)
	}
}

func (n *Node) handleStatusRequest(msg *pb.StatusRequest) {
	log.WithField("request", msg.String()).Info("Received status request")

	statusResponse := &pb.StatusResponse{
		LastStableSequenceNumber: n.Store.GetLastStableCheckpoint().GetSequenceNumber(),
		CheckpointProof:          n.Store.GetLastStableCheckpoint().GetProof(),
	}

	n.sender.SendRPCToPeer(msg.ReplicaId, "Status", statusResponse)
}

func (n *Node) handleStatusResponse(msg *pb.StatusResponse) {
	log.WithField("request", msg.String()).Info("Received status response")

	if !n.verifyStatusResponse(msg) {
		log.WithField("request", msg.String()).Info("Failed to verify status response")
		return
	}

	if msg.LastStableSequenceNumber > n.Store.GetLastStableCheckpoint().GetSequenceNumber() {
		n.Store.UpdateLastStableCheckpoint(msg.CheckpointProof)
		n.ViewData.LastSequenceNumber = msg.LastStableSequenceNumber
	}
	maxView := int64(0)
	for _, p := range msg.CheckpointProof {
		if p.ViewId > maxView {
			maxView = p.ViewId
		}
	}
	n.ViewData.CurrentView = maxView
	n.ViewData.LeaderId = n.LeaderElection.GetLeader(n.ViewData.CurrentView)
}

func (n *Node) verifyPrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) bool {
	// TODO: check signature
	if msg.PrePrepareRequest.ViewId != n.ViewData.CurrentView {
		log.WithField("preprepare", msg.String()).WithField("my-view", n.ViewData.CurrentView).Warn("preprepare view mismatch")
		return false
	}

	sequenceNumber := msg.PrePrepareRequest.SequenceNumber
	pastPreprepare := n.ViewData.Preprepares[sequenceNumber]
	if pastPreprepare != nil && pastPreprepare.RequestDigest != msg.PrePrepareRequest.RequestDigest {
		log.WithField("preprepare", msg.String()).WithField("my-digest", pastPreprepare.RequestDigest).Warn("preprepare digest mismatch")
		return false
	}

	if !n.sequenceInWaterMark(sequenceNumber) {
		return false
	}

	return true
}

func (n *Node) verifyPrepareRequest(msg *pb.PrepareRequest) bool {
	// TODO: check signature
	if msg.ViewId != n.ViewData.CurrentView {
		log.WithField("prepare", msg.String()).WithField("my-view", n.ViewData.CurrentView).Warn("prepare view mismatch")
		return false
	}

	sequenceNumber := msg.SequenceNumber
	pastPreprepare := n.ViewData.Preprepares[sequenceNumber]
	if pastPreprepare == nil {
		log.WithField("prepare", msg.String()).Warn("prepare without preprepare")
		return false
	}

	if pastPreprepare.RequestDigest != msg.RequestDigest {
		log.WithField("prepare", msg.String()).WithField("my-digest", pastPreprepare.RequestDigest).Warn("prepare digest mismatch")
		return false
	}

	if !n.sequenceInWaterMark(sequenceNumber) {
		return false
	}

	return true
}

func (n *Node) verifyCommitRequest(msg *pb.CommitRequest) bool {
	// TODO: check signature
	if msg.ViewId != n.ViewData.CurrentView {
		log.WithField("commit", msg.String()).WithField("my-view", n.ViewData.CurrentView).Warn("commit view mismatch")
		return false
	}

	sequenceNumber := msg.SequenceNumber
	pastPreprepare := n.ViewData.Preprepares[sequenceNumber]
	if pastPreprepare == nil {
		log.WithField("commit", msg.String()).Warn("commit without preprepare")
		return false
	}

	if pastPreprepare.RequestDigest != msg.RequestDigest {
		log.WithField("commit", msg.String()).WithField("my-digest", pastPreprepare.RequestDigest).Warn("commit digest mismatch")
		return false
	}

	if !n.sequenceInWaterMark(sequenceNumber) {
		return false
	}

	return true
}

func (n *Node) verifyStatusResponse(msg *pb.StatusResponse) bool {
	// TODO: complete verification
	if msg.LastStableSequenceNumber < n.Store.GetLastStableCheckpoint().GetSequenceNumber() {
		return false
	}
	if len(msg.CheckpointProof) == 0 {
		return false
	}

	return true
}

func (n *Node) sequenceInWaterMark(sequenceNumber int64) bool {
	lowWaterMark := n.Store.GetLastStableCheckpoint().GetSequenceNumber() + 1
	highWaterMark := lowWaterMark + int64(n.config.General.WaterMarkInterval)
	if sequenceNumber < lowWaterMark || sequenceNumber >= highWaterMark {
		log.WithField("sequence-number", sequenceNumber).
			WithField("low-water-mark", lowWaterMark).
			WithField("high-water-mark", highWaterMark).
			Warn("watermark mismatch")
		return false
	}

	return true
}
