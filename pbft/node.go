package pbft

import (
	"time"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type Role string

const (
	Primary Role = "Primary"
	Backup  Role = "Backup"
)

type Node struct {
	Config    *Config
	Sender    *Sender
	Role      Role
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
}

func NewNode(
	config *Config,
	sender *Sender,
	inputCh <-chan proto.Message,
	requestCh <-chan *pb.ClientRequest,
	enableCh <-chan any,
	disableCh <-chan any,
) *Node {
	leaderElection := NewRoundRobinLeaderElection(config)
	role := Backup
	if leaderElection.GetLeader(0) == config.Id {
		role = Primary
	}
	return &Node{
		Config:    config,
		Sender:    sender,
		Role:      role,
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
	}
}

func (n *Node) Run() {
	n.ViewChangeTimer = time.NewTimer(time.Second)
	n.ViewChangeTimer.Stop()

	clientRequestTicker := time.NewTicker(10 * time.Millisecond)
	var requestBatch []*pb.ClientRequest
	for {
		if !n.Enabled {
			<-n.EnableCh
			n.Enabled = true
			exhaustChannel(n.DisableCh)
		}

		select {
		case request := <-n.RequestCh:
			if len(requestBatch) > 40 {
				continue
			}
			requestBatch = append(requestBatch, request)
		case input := <-n.InputCh:
			n.handleInput(input)
		case <-n.ViewChangeTimer.C:
			n.RunningTimer = false
			n.goToViewChange()
		case <-clientRequestTicker.C:
			if len(requestBatch) == 0 {
				continue
			}
			n.handleClientRequest(requestBatch)
			requestBatch = []*pb.ClientRequest{}
		case <-n.DisableCh:
			n.Enabled = false
			exhaustChannel(n.EnableCh)
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

func (n *Node) setViewChangeTimer() {
	if n.Role == Primary {
		n.RunningTimer = false
		n.ViewChangeTimer.Stop()
		return
	}

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
	}
}

func (n *Node) handleClientRequest(msgs []*pb.ClientRequest) {
	if n.Role != Primary {
		log.WithField("request", msgs[0].String()).Warn("Received client request but not primary")
		log.WithField("leader", n.LeaderId).Info("Forwarding request to leader")
		for _, msg := range msgs {
			n.Sender.SendRPCToPeer(n.LeaderId, "Request", msg)
		}

		if !n.RunningTimer && !n.IsInViewChange {
			n.setViewChangeTimer()
		}

		return
	}

	if n.IsInViewChange {
		log.Warn("Dismissing request because in view change")
		if !n.RunningTimer {
			n.setViewChangeTimer()
		}
		return
	}

	log.WithField("request", msgs[0].String()).Info("Received client request")

	if len(n.InProgressRequests) >= n.Config.General.MaxOutstandingRequests {
		log.Warn("Too many outstanding requests, putting request in pending queue")
		n.PendingRequests = append(n.PendingRequests, msgs...)
		return
	}

	n.LastSequenceNumber++
	n.InProgressRequests[n.LastSequenceNumber] = struct{}{}
	prepreareMessage := &pb.PiggyBackedPrePareRequest{
		PrePrepareRequest: &pb.PrePrepareRequest{
			ViewId:         n.CurrentView,
			SequenceNumber: n.LastSequenceNumber,
		},
		Requests: msgs,
	}
	n.Preprepares[n.LastSequenceNumber] = prepreareMessage.PrePrepareRequest

	n.Store.AddRequests(n.LastSequenceNumber, msgs)
	n.Sender.Broadcast("PrePrepare", prepreareMessage)
}

func (n *Node) handlePrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) {
	if n.Role == Primary {
		log.WithField("request", msg.String()).Warn("Received pre-prepare request but is primary")
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

	// prepared
	if len(n.Prepares[sequenceNumber]) == 2*n.Config.F()+1 {
		commitMessage := &pb.CommitRequest{
			ViewId:         msg.ViewId,
			SequenceNumber: msg.SequenceNumber,
			RequestDigest:  msg.RequestDigest,
			ReplicaId:      n.Config.Id,
		}

		n.Commits[sequenceNumber] = make(map[string]*pb.CommitRequest)
		n.Commits[sequenceNumber][n.Config.Id] = commitMessage
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

	// committed
	if len(n.Commits[sequenceNumber]) == 2*n.Config.F()+1 {
		reqs, resps, checkpoint := n.Store.Commit(msg)

		delete(n.InProgressRequests, sequenceNumber)
		if len(n.InProgressRequests) == 0 {
			n.ViewChangeTimer.Stop()
		}

		if checkpoint != nil {
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
			n.handleClientRequest(n.PendingRequests)
			n.PendingRequests = []*pb.ClientRequest{}
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
	if len(n.InProgressRequests) == 0 {
		n.ViewChangeTimer.Stop()
	}
}

func (n *Node) goToViewChange() {
	log.WithField("node id", n.Config.Id).WithField("leader", n.LeaderElection.GetLeader(n.CurrentView)).Error("view changing")
	n.setViewChangeTimer()
	n.IsInViewChange = true
	n.CurrentView++
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

	if n.LeaderElection.GetLeader(n.CurrentView) == n.Config.Id {
		n.handleViewChangeRequest(viewChangeRequest)
	} else {
		n.Sender.SendRPCToPeer(
			n.LeaderElection.GetLeader(n.CurrentView),
			"ViewChange",
			viewChangeRequest,
		)
	}
}

func (n *Node) handleViewChangeRequest(msg *pb.ViewChangeRequest) {
	log.WithField("request", msg.String()).Info("Received view change request")
	log.WithField("backup id", msg.ReplicaId).Error("received view change request")

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
	log.WithField("backup id", msg.ReplicaId).WithField("len", len(n.ViewChanges[msg.NewViewId])).Error("applied view change message")

	if len(n.ViewChanges[msg.NewViewId]) == 2*n.Config.F()+1 {
		minSeq := n.Store.GetLastStableSequenceNumber()
		maxSeq := int64(0)
		for _, viewChange := range n.ViewChanges[msg.NewViewId] {
			if viewChange.LastStableSequenceNumber < minSeq {
				minSeq = viewChange.LastStableSequenceNumber
			}
			if viewChange.LastStableSequenceNumber > maxSeq {
				maxSeq = viewChange.LastStableSequenceNumber
			}
		}

		preprepares := make([]*pb.PrePrepareRequest, maxSeq-minSeq)
		for seqNo := minSeq + 1; seqNo < maxSeq; seqNo++ {
			if preprepare, ok := n.Preprepares[int64(seqNo)]; ok {
				preprepares[seqNo-minSeq] = preprepare
			} else {
				preprepares[seqNo-minSeq] = &pb.PrePrepareRequest{
					ViewId:         msg.NewViewId - 1,
					SequenceNumber: seqNo,
					RequestDigest:  "nil",
				}
			}
		}

		viewChangeMessages := make([]*pb.ViewChangeRequest, 0, 2*n.Config.F())
		for _, viewChange := range n.ViewChanges[msg.NewViewId] {
			viewChangeMessages = append(viewChangeMessages, viewChange)
		}

		newViewMessage := &pb.NewViewRequest{
			NewViewId:       msg.NewViewId,
			ViewChangeProof: viewChangeMessages,
			Preprepares:     preprepares,
			ReplicaId:       n.Config.Id,
		}

		n.Sender.Broadcast("NewView", newViewMessage)
	}
}

func (n *Node) handleNewViewRequest(msg *pb.NewViewRequest) {
	log.WithField("request", msg.String()).Info("Received new view request")

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
	n.IsInViewChange = false
	n.ViewChangeTimer.Stop()
	select {
	// drain the channel if there is a message
	case <-n.ViewChangeTimer.C:
	default:
	}
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
