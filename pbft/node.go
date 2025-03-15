package pbft

//go:generate mockgen -source=node.go -destination=node_mock.go -package=pbft

import (
	"time"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ISender interface {
	SendRPCToClient(clientAddress, method string, message proto.Message)
	SendRPCToPeer(peerID string, method string, message proto.Message)
	Broadcast(method string, message proto.Message)
}

type Node struct {
	Config    *Config
	Sender    ISender
	Store     *Store
	InputCh   <-chan proto.Message
	RequestCh <-chan *pb.ClientRequest

	LeaderElection LeaderElection

	RunningTimer    bool
	ViewChangeTimer *time.Timer
	IsInViewChange  bool
	ViewChanges     map[int64]map[string]*pb.ViewChangeRequest

	Preprepares map[int64]*pb.PrePrepareRequest
	Prepares    map[int64]map[string]*pb.PrepareRequest
	Commits     map[int64]map[string]*pb.CommitRequest

	PendingRequests    []*pb.ClientRequest
	InProgressRequests map[int64]any

	CurrentView        int64
	LeaderId           string
	LastSequenceNumber int64

	Enabled   bool
	EnableCh  <-chan any
	DisableCh <-chan any
	StopCh    chan any
}

func NewNode(
	config *Config,
	sender ISender,
	inputCh <-chan proto.Message,
	requestCh <-chan *pb.ClientRequest,
	enableCh <-chan any,
	disableCh <-chan any,
) *Node {
	leaderElection := NewRoundRobinLeaderElection(config)
	return &Node{
		Config:    config,
		Sender:    sender,
		Store:     NewStore(config),
		InputCh:   inputCh,
		RequestCh: requestCh,

		LeaderElection: leaderElection,

		RunningTimer:   false,
		IsInViewChange: false,
		ViewChanges:    make(map[int64]map[string]*pb.ViewChangeRequest),

		Preprepares: make(map[int64]*pb.PrePrepareRequest),
		Prepares:    make(map[int64]map[string]*pb.PrepareRequest),
		Commits:     make(map[int64]map[string]*pb.CommitRequest),

		PendingRequests:    []*pb.ClientRequest{},
		InProgressRequests: make(map[int64]any),

		CurrentView: 0,
		LeaderId:    leaderElection.GetLeader(0),

		Enabled:   config.General.EnabledByDefault,
		EnableCh:  enableCh,
		DisableCh: disableCh,
		StopCh:    make(chan any),
	}
}

func (n *Node) Run() {
	n.ViewChangeTimer = time.NewTimer(time.Second)
	n.ViewChangeTimer.Stop()

	n.Sender.Broadcast("GetStatus", &pb.StatusRequest{ReplicaId: n.Config.Id})
	for {
		if !n.Enabled {
			<-n.EnableCh
			n.Enabled = true
			exhaustChannel(n.DisableCh)
			n.Sender.Broadcast("GetStatus", &pb.StatusRequest{ReplicaId: n.Config.Id})
		}

		select {
		case request := <-n.RequestCh:
			n.handleClientRequest(request)
		case input := <-n.InputCh:
			n.handleInput(input)
		case <-n.ViewChangeTimer.C:
			n.goToViewChange()
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
	return n.Config.Id == n.LeaderId
}

func (n *Node) setViewChangeTimer() {
	if n.isPrimary() {
		log.WithField("my-id", n.Config.Id).Info("stopping view change timer")
		n.RunningTimer = false
		n.ViewChangeTimer.Stop()
		return
	}

	log.WithField("my-id", n.Config.Id).
		WithField("leader", n.LeaderId).
		WithField("duration", time.Duration(n.Config.Timers.ViewChangeTimeoutMs)*time.Millisecond).
		Info("setting view change timer")
	n.RunningTimer = true
	n.ViewChangeTimer.Reset(time.Duration(n.Config.Timers.ViewChangeTimeoutMs) * time.Millisecond)
}

func (n *Node) handleInput(input proto.Message) {
	switch msg := input.(type) {
	case *pb.PiggyBackedPrePareRequest:
		n.handlePrePrepareRequest(msg)
	case *pb.PrepareRequest:
		n.handlePrepareRequest(msg)
	case *pb.CommitRequest:
		n.handleCommitRequest(msg)
	case *pb.CheckpointRequest:
		n.handleCheckpointRequest(msg)
	case *pb.ViewChangeRequest:
		n.handleViewChangeRequest(msg)
	case *pb.NewViewRequest:
		n.handleNewViewRequest(msg)
	case *pb.StatusRequest:
		n.handleStatusRequest(msg)
	case *pb.StatusResponse:
		n.handleStatusResponse(msg)
	}
}

func (n *Node) handleClientRequest(msg *pb.ClientRequest) {
	if !n.isPrimary() {
		log.WithField("request", msg.String()).Info("Received client request but not primary")
		log.WithField("my-id", n.Config.Id).WithField("leader", n.LeaderId).Info("Forwarding request to leader")
		n.Sender.SendRPCToPeer(n.LeaderId, "Request", msg)
		return
	}

	if n.IsInViewChange {
		log.Warn("Dismissing request because in view change")
		if !n.RunningTimer {
			n.setViewChangeTimer()
		}
		return
	}

	log.WithField("request", msg.String()).Info("Received client request")

	if len(n.InProgressRequests) >= n.Config.General.MaxOutstandingRequests {
		log.Warn("Too many outstanding requests, putting request in pending queue")
		n.PendingRequests = append(n.PendingRequests, msg)
		return
	}

	n.LastSequenceNumber++
	n.InProgressRequests[n.LastSequenceNumber] = struct{}{}
	prepreareMessage := &pb.PiggyBackedPrePareRequest{
		PrePrepareRequest: &pb.PrePrepareRequest{
			ViewId:         n.CurrentView,
			SequenceNumber: n.LastSequenceNumber,
		},
		Requests: []*pb.ClientRequest{msg},
	}
	n.Preprepares[n.LastSequenceNumber] = prepreareMessage.PrePrepareRequest

	n.Store.AddRequests(n.LastSequenceNumber, []*pb.ClientRequest{msg})
	n.Sender.Broadcast("PrePrepare", prepreareMessage)
}

func (n *Node) handlePrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) {
	//log.WithField("id", msg.PrePrepareRequest.SequenceNumber).WithField("my-id", n.Config.Id).Error("PrePrepare received")
	time.Sleep(1 * time.Millisecond)
	if n.isPrimary() {
		log.WithField("request", msg.String()).WithField("my-id", n.Config.Id).Warn("Received pre-prepare request but is primary")
		return
	}

	if n.IsInViewChange {
		log.Warn("Dismissing preprepare because in view change")
		if !n.RunningTimer {
			n.setViewChangeTimer()
		}
		return
	}

	log.WithField("request", msg.String()).Info("Received pre-prepare request")

	n.setViewChangeTimer()
	if !n.verifyPrePrepareRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify pre-prepare request")
		return
	}

	sequenceNumber := msg.PrePrepareRequest.SequenceNumber
	n.InProgressRequests[sequenceNumber] = struct{}{}
	n.Preprepares[sequenceNumber] = msg.PrePrepareRequest

	prepareMessage := &pb.PrepareRequest{
		ViewId:         msg.PrePrepareRequest.ViewId,
		SequenceNumber: sequenceNumber,
		RequestDigest:  msg.PrePrepareRequest.RequestDigest,
		ReplicaId:      n.Config.Id,
	}

	n.Prepares[sequenceNumber] = make(map[string]*pb.PrepareRequest)
	n.Prepares[sequenceNumber][n.Config.Id] = prepareMessage

	n.Store.AddRequests(msg.PrePrepareRequest.SequenceNumber, msg.Requests)
	n.Sender.Broadcast("Prepare", prepareMessage)
}

func (n *Node) handlePrepareRequest(msg *pb.PrepareRequest) {
	log.WithField("request", msg.String()).Info("Received prepare request")

	if n.IsInViewChange {
		log.Warn("Dismissing prepare because in view change")
		if !n.RunningTimer {
			n.setViewChangeTimer()
		}
		return
	}

	if !n.verifyPrepareRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify prepare request")
		return
	}

	sequenceNumber := msg.SequenceNumber

	if _, ok := n.Prepares[sequenceNumber]; !ok {
		n.Prepares[sequenceNumber] = make(map[string]*pb.PrepareRequest)
	}

	n.Prepares[sequenceNumber][msg.ReplicaId] = msg

	prepareNodes := make([]string, 0)
	for id := range n.Prepares[sequenceNumber] {
		prepareNodes = append(prepareNodes, id)
	}
	log.WithField("prepare-nodes", prepareNodes).Info("prepare nodes")

	// prepared
	if len(n.Prepares[sequenceNumber]) == 2*n.Config.F() {
		commitMessage := &pb.CommitRequest{
			ViewId:         msg.ViewId,
			SequenceNumber: msg.SequenceNumber,
			RequestDigest:  msg.RequestDigest,
			ReplicaId:      n.Config.Id,
		}

		n.handleCommitRequest(commitMessage)
		n.Sender.Broadcast("Commit", commitMessage)
	}
}

func (n *Node) handleCommitRequest(msg *pb.CommitRequest) {
	log.WithField("request", msg.String()).Info("Received commit request")

	if n.IsInViewChange {
		log.Warn("Dismissing commit because in view change")
		if !n.RunningTimer {
			n.setViewChangeTimer()
		}
		return
	}

	if !n.verifyCommitRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify commit request")
		return
	}

	sequenceNumber := msg.SequenceNumber
	if _, ok := n.Commits[sequenceNumber]; !ok {
		n.Commits[sequenceNumber] = make(map[string]*pb.CommitRequest)
	}

	n.Commits[sequenceNumber][msg.ReplicaId] = msg

	committeds := make([]string, 0)
	for id := range n.Commits[sequenceNumber] {
		committeds = append(committeds, id)
	}
	log.WithField("committed-nodes", committeds).Info("committed nodes")

	// committed
	if len(n.Commits[sequenceNumber]) == 2*n.Config.F()+1 {
		reqs, resps, checkpoints := n.Store.Commit(msg)

		delete(n.InProgressRequests, sequenceNumber)

		for _, checkpoint := range checkpoints {
			checkpoint.ViewId = n.CurrentView
			n.handleCheckpointRequest(checkpoint)
			n.Sender.Broadcast("Checkpoint", checkpoint)
		}

		for i, req := range reqs {
			reply := &pb.ClientResponse{
				ViewId:      msg.ViewId,
				TimestampMs: req.TimestampMs,
				ClientId:    req.ClientId,
				ReplicaId:   n.Config.Id,
				Result:      resps[i],
			}
			n.Sender.SendRPCToClient(req.Callback, "Response", reply)
		}

		if len(n.InProgressRequests) < n.Config.General.MaxOutstandingRequests && len(n.PendingRequests) > 0 {
			pendings := n.PendingRequests
			n.PendingRequests = []*pb.ClientRequest{}
			for _, pending := range pendings {
				n.handleClientRequest(pending)
			}
		}
	}
}

func (n *Node) handleCheckpointRequest(msg *pb.CheckpointRequest) {
	log.WithField("request", msg.String()).Info("Received checkpoint request")

	stableSequenceNumber := n.Store.AddCheckpointRequest(msg)
	if stableSequenceNumber == nil {
		return
	}

	for seqNo := range n.Preprepares {
		if seqNo <= *stableSequenceNumber {
			delete(n.Preprepares, seqNo)
		}
	}

	for seqNo := range n.Prepares {
		if seqNo <= *stableSequenceNumber {
			delete(n.Prepares, seqNo)
		}
	}

	for seqNo := range n.Commits {
		if seqNo <= *stableSequenceNumber {
			delete(n.Commits, seqNo)
		}
	}

	for req := range n.InProgressRequests {
		if req <= *stableSequenceNumber {
			delete(n.InProgressRequests, req)
		}
	}
}

func (n *Node) goToViewChange() {
	if n.isPrimary() && !n.IsInViewChange {
		return
	}

	log.WithField("node id", n.Config.Id).WithField("leader", n.LeaderElection.GetLeader(n.CurrentView)).Error("view changing")
	n.setViewChangeTimer()
	n.IsInViewChange = true
	n.CurrentView++
	n.LeaderId = n.LeaderElection.GetLeader(n.CurrentView)
	var prepreparedProof []*pb.ViewChangePreparedMessage

	for seqNo := range n.Commits {
		preprepare := n.Preprepares[seqNo]
		var prepareMessages []*pb.PrepareRequest
		if len(n.Prepares[seqNo]) > n.Config.F()*2+1 {
			break
		}
		for _, prepare := range n.Prepares[seqNo] {
			prepareMessages = append(prepareMessages, prepare)
		}

		preparedProof := &pb.ViewChangePreparedMessage{
			PrePrepareRequest: preprepare,
			PreparedMessages:  prepareMessages,
		}
		prepreparedProof = append(prepreparedProof, preparedProof)
	}

	viewChangeRequest := &pb.ViewChangeRequest{
		NewViewId:                n.CurrentView,
		LastStableSequenceNumber: n.Store.GetLastStableSequenceNumber(),
		CheckpointProof:          n.Store.GetLastStableCheckpoint(),
		PreparedProof:            prepreparedProof,
		ReplicaId:                n.Config.Id,
	}

	if n.LeaderId == n.Config.Id {
		n.handleViewChangeRequest(viewChangeRequest)
	}
	n.Sender.Broadcast("ViewChange", viewChangeRequest)
}

func (n *Node) handleViewChangeRequest(msg *pb.ViewChangeRequest) {
	log.WithField("my-id", n.Config.Id).WithField("backup id", msg.ReplicaId).Error("received view change request")

	if msg.NewViewId < n.CurrentView {
		log.WithField("request", msg.String()).WithField("current-view", n.CurrentView).Warn("Received view change request with old view")
		return
	}

	if n.LeaderElection.GetLeader(msg.NewViewId) != n.Config.Id {
		log.WithField("request", msg.String()).Warn("Received view change request but not leader for view")
		return
	}

	if _, ok := n.ViewChanges[msg.NewViewId]; !ok {
		n.ViewChanges[msg.NewViewId] = make(map[string]*pb.ViewChangeRequest)
	}
	n.ViewChanges[msg.NewViewId][msg.ReplicaId] = msg
	log.WithField("backup id", msg.ReplicaId).WithField("len", len(n.ViewChanges[msg.NewViewId])).WithField("2f+1", 2*n.Config.F()+1).Error("applied view change message")

	if len(n.ViewChanges[msg.NewViewId]) == 2*n.Config.F()+1 {
		minSeq := n.Store.GetLastStableSequenceNumber()
		maxSeq := int64(0)

		// Determine minSeq and maxSeq from all ViewChange messages
		for _, viewChange := range n.ViewChanges[msg.NewViewId] {
			if viewChange.LastStableSequenceNumber < minSeq {
				minSeq = viewChange.LastStableSequenceNumber
			}
			if viewChange.LastStableSequenceNumber > maxSeq {
				maxSeq = viewChange.LastStableSequenceNumber
			}
			for _, preparedProof := range viewChange.PreparedProof {
				if preparedProof.PrePrepareRequest.SequenceNumber > maxSeq {
					maxSeq = preparedProof.PrePrepareRequest.SequenceNumber
				}
			}
		}

		// Aggregate highest prepared certificates from all ViewChange messages
		highestPrepares := make(map[int64]*pb.PrePrepareRequest)
		for _, viewChange := range n.ViewChanges[msg.NewViewId] {
			for _, preparedProof := range viewChange.PreparedProof {
				prePrepare := preparedProof.PrePrepareRequest
				seqNo := prePrepare.SequenceNumber

				if seqNo < minSeq+1 || seqNo > maxSeq {
					continue
				}

				// Validate Prepare messages count
				replicaIDs := make(map[string]struct{})
				for _, prepare := range preparedProof.PreparedMessages {
					if prepare.SequenceNumber == seqNo && prepare.RequestDigest == prePrepare.RequestDigest {
						replicaIDs[prepare.ReplicaId] = struct{}{}
					}
				}
				if len(replicaIDs) < 2*n.Config.F() {
					continue
				}

				// Update highest view PrePrepare for this sequence number
				if current, exists := highestPrepares[seqNo]; !exists || prePrepare.ViewId > current.ViewId {
					highestPrepares[seqNo] = prePrepare
				}
			}
		}

		// Generate new PrePrepares for the new view
		preprepares := make([]*pb.PrePrepareRequest, 0)
		for seqNo := minSeq + 1; seqNo <= maxSeq; seqNo++ {
			var digest string
			if prePrepare, exists := highestPrepares[seqNo]; exists {
				digest = prePrepare.RequestDigest
			} else {
				digest = "nil"
			}

			preprepares = append(preprepares, &pb.PrePrepareRequest{
				ViewId:         msg.NewViewId,
				SequenceNumber: seqNo,
				RequestDigest:  digest,
			})
		}

		// Collect view change proofs
		viewChangeMessages := make([]*pb.ViewChangeRequest, 0, len(n.ViewChanges[msg.NewViewId]))
		for _, vc := range n.ViewChanges[msg.NewViewId] {
			viewChangeMessages = append(viewChangeMessages, vc)
		}

		// Broadcast NewView message
		newViewMessage := &pb.NewViewRequest{
			NewViewId:       msg.NewViewId,
			ViewChangeProof: viewChangeMessages,
			Preprepares:     preprepares,
			ReplicaId:       n.Config.Id,
		}
		n.Sender.Broadcast("NewView", newViewMessage)
		log.WithField("my-id", n.Config.Id).Error("broadcasted new view message")

		// Update node state
		n.IsInViewChange = false
		n.CurrentView = msg.NewViewId
		n.LeaderId = n.Config.Id
		n.LastSequenceNumber = maxSeq
		n.setViewChangeTimer()
	}
}

func (n *Node) handleNewViewRequest(msg *pb.NewViewRequest) {
	log.WithField("my-id", n.Config.Id).Info("Received new view request")

	if msg.NewViewId < n.CurrentView {
		log.WithField("request", msg.String()).WithField("current-view", n.CurrentView).Warn("Received new view request with old view")
		return
	}

	if !n.verifyNewViewRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify new view request")
		return
	}

	n.CurrentView = msg.NewViewId
	n.LeaderId = msg.ReplicaId
	log.WithField("my-id", n.Config.Id).WithField("leader-id", n.LeaderId).Error("entered new view")
	n.IsInViewChange = false
	n.setViewChangeTimer()
	n.ViewChanges = make(map[int64]map[string]*pb.ViewChangeRequest)
	n.Preprepares = make(map[int64]*pb.PrePrepareRequest)
	n.Prepares = make(map[int64]map[string]*pb.PrepareRequest)
	n.Commits = make(map[int64]map[string]*pb.CommitRequest)

	for _, preprepare := range msg.Preprepares {
		n.Preprepares[preprepare.SequenceNumber] = preprepare
		n.Prepares[preprepare.SequenceNumber] = make(map[string]*pb.PrepareRequest)
		prepareMessage := &pb.PrepareRequest{
			ViewId:         preprepare.ViewId,
			SequenceNumber: preprepare.SequenceNumber,
			RequestDigest:  preprepare.RequestDigest,
			ReplicaId:      n.Config.Id,
		}
		n.Prepares[preprepare.SequenceNumber][n.Config.Id] = prepareMessage
		n.Sender.Broadcast("Prepare", prepareMessage)
	}
}

func (n *Node) handleStatusRequest(msg *pb.StatusRequest) {
	log.WithField("request", msg.String()).Info("Received status request")

	statusResponse := &pb.StatusResponse{
		LastStableSequenceNumber: n.Store.GetLastStableSequenceNumber(),
		CheckpointProof:          n.Store.GetLastStableCheckpoint(),
	}

	n.Sender.SendRPCToPeer(msg.ReplicaId, "Status", statusResponse)
}

func (n *Node) handleStatusResponse(msg *pb.StatusResponse) {
	log.WithField("request", msg.String()).Info("Received status response")

	if !n.verifyStatusResponse(msg) {
		log.WithField("request", msg.String()).Info("Failed to verify status response")
		return
	}

	if msg.LastStableSequenceNumber > n.Store.GetLastStableSequenceNumber() {
		n.Store.UpdateLastStableCheckpoint(msg.CheckpointProof)
		n.LastSequenceNumber = msg.LastStableSequenceNumber
	}
	maxView := int64(0)
	for _, p := range msg.CheckpointProof {
		if p.ViewId > maxView {
			maxView = p.ViewId
		}
	}
	n.CurrentView = maxView
}

func (n *Node) verifyPrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) bool {
	// TODO: check signature
	if msg.PrePrepareRequest.ViewId != n.CurrentView {
		log.WithField("preprepare", msg.String()).WithField("my-view", n.CurrentView).Warn("preprepare view mismatch")
		return false
	}

	sequenceNumber := msg.PrePrepareRequest.SequenceNumber
	pastPreprepare := n.Preprepares[sequenceNumber]
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
	if msg.ViewId != n.CurrentView {
		log.WithField("prepare", msg.String()).WithField("my-view", n.CurrentView).Warn("prepare view mismatch")
		return false
	}

	sequenceNumber := msg.SequenceNumber
	pastPreprepare := n.Preprepares[sequenceNumber]
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
	if msg.ViewId != n.CurrentView {
		log.WithField("commit", msg.String()).WithField("my-view", n.CurrentView).Warn("commit view mismatch")
		return false
	}

	sequenceNumber := msg.SequenceNumber
	pastPreprepare := n.Preprepares[sequenceNumber]
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

func (n *Node) verifyNewViewRequest(msg *pb.NewViewRequest) bool {
	if msg.NewViewId < n.CurrentView {
		return false
	}

	// TODO: A backup accepts a new-view message for view v + 1
	// if it is signed properly, if the view-change messages it
	// contains are valid for view v + 1, and if the set O is
	// correct; it verifies the correctness of O by performing a
	// computation similar to the one used by the primary to
	// create O. Then it adds the new information to its log as
	// described for the primary, multicasts a prepare for each
	// message in to all the other replicas, adds these prepares
	// to its log, and enters view v + 1.
	return true
}

func (n *Node) verifyStatusResponse(msg *pb.StatusResponse) bool {
	// TODO: complete verification
	if msg.LastStableSequenceNumber < n.Store.GetLastStableSequenceNumber() {
		return false
	}
	if len(msg.CheckpointProof) == 0 {
		return false
	}

	return true
}

func (n *Node) sequenceInWaterMark(sequenceNumber int64) bool {
	lowWaterMark := n.Store.GetLastStableSequenceNumber() + 1
	highWaterMark := lowWaterMark + int64(n.Config.General.WaterMarkInterval)
	if sequenceNumber < lowWaterMark || sequenceNumber >= highWaterMark {
		log.WithField("sequence-number", sequenceNumber).
			WithField("low-water-mark", lowWaterMark).
			WithField("high-water-mark", highWaterMark).
			Warn("watermark mismatch")
		return false
	}

	return true
}
