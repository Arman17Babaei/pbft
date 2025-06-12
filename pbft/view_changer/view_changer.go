package view_changer

import (
	"sync/atomic"
	"time"

	"github.com/Arman17Babaei/pbft/pbft"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ISender interface {
	SendRPCToPeer(peerID string, method string, message proto.Message)
	Broadcast(method string, message proto.Message)
}

type Node interface {
	GetCurrentPreparedRequests() []*pb.ViewChangePreparedMessage
	GetLeaderForView(viewId int64) string
	HandleNewViewRequest(msg *pb.NewViewRequest)
	GoToViewChange()
}

type PbftViewChange struct {
	id                    string
	baseViewChangeTimeout time.Duration
	requestTimeout        time.Duration

	store  *pbft.Store
	node   Node
	config *configs.Config
	sender ISender

	viewId                   atomic.Int64
	inViewChange             bool
	currentViewChangeTimeout time.Duration
	viewTimer                *time.Timer

	viewChanges map[int64]map[string]*pb.ViewChangeRequest
}

func NewPbftViewChange(config *configs.Config, store *pbft.Store, sender ISender) *PbftViewChange {
	viewChangeTimeout := time.Duration(config.Timers.ViewChangeTimeoutMs) * time.Millisecond
	requestTimeout := time.Duration(config.Timers.RequestTimeoutMs) * time.Millisecond
	p := &PbftViewChange{
		id:                    config.Id,
		baseViewChangeTimeout: viewChangeTimeout,
		requestTimeout:        requestTimeout,

		store:  store,
		config: config,
		sender: sender,

		viewId:                   atomic.Int64{},
		inViewChange:             false,
		currentViewChangeTimeout: viewChangeTimeout,
		viewTimer:                time.NewTimer(viewChangeTimeout),
		viewChanges:              make(map[int64]map[string]*pb.ViewChangeRequest),
	}
	return p
}

func (p *PbftViewChange) SetNode(node Node) {
	p.node = node
}

func (p *PbftViewChange) Run(viewChangeCh <-chan proto.Message) {
	go p.runTimer()
	for msg := range viewChangeCh {
		switch m := msg.(type) {
		case *pb.ViewChangeRequest:
			p.ReceiveViewChange(m)
		case *pb.NewViewRequest:
			p.handleNewView(m)
		}
	}
}

func (p *PbftViewChange) runTimer() {
	for {
		select {
		case <-p.viewTimer.C:
			p.inViewChange = true
			p.viewId.Add(1)
			p.node.GoToViewChange()
			go p.voteViewChange()
			p.viewTimer.Reset(p.currentViewChangeTimeout)
			p.currentViewChangeTimeout = p.currentViewChangeTimeout * 2
		}
	}
}

func (p *PbftViewChange) handleNewView(msg *pb.NewViewRequest) {
	if msg.NewViewId < p.viewId.Load() {
		log.WithField("request", msg.String()).WithField("current-view", p.viewId.Load()).Warn("Received new view request with old view")
		return
	}

	p.viewId.Store(msg.NewViewId)
	p.inViewChange = false
	p.viewTimer.Reset(p.requestTimeout)
	p.currentViewChangeTimeout = p.baseViewChangeTimeout
	p.node.HandleNewViewRequest(msg)
}

func (p *PbftViewChange) RequestExecuted(viewId int64) {
	if p.inViewChange {
		monitoring.ErrorCounter.WithLabelValues("pbft_view_changer", "RequestReceived", "in_view_change").Inc()
		return
	}
	if p.viewId.Load() != viewId {
		monitoring.ErrorCounter.WithLabelValues("pbft_view_changer", "RequestReceived", "invalid_view_id").Inc()
		return
	}

	p.viewTimer.Reset(p.requestTimeout)
}

func (p *PbftViewChange) ReceiveViewChange(msg *pb.ViewChangeRequest) {
	if msg.NewViewId < p.viewId.Load() {
		log.WithField("request", msg.String()).WithField("current-view", p.viewId.Load()).Warn("Received view change request with old view")
		return
	}

	p.handleViewChange(msg)
}

func (p *PbftViewChange) voteViewChange() {
	log.WithField("node id", p.id).Error("view changing")
	stableCheckpoint := p.store.GetLastStableCheckpoint()
	viewChangeRequest := &pb.ViewChangeRequest{
		NewViewId:                p.viewId.Load(),
		LastStableSequenceNumber: stableCheckpoint.GetSequenceNumber(),
		CheckpointProof:          stableCheckpoint.GetProof(),
		PreparedProof:            p.node.GetCurrentPreparedRequests(),
		ReplicaId:                p.id,
	}

	p.handleViewChange(viewChangeRequest)
	p.sender.Broadcast("ViewChange", viewChangeRequest)
}

func (p *PbftViewChange) handleViewChange(msg *pb.ViewChangeRequest) {
	log.WithField("my-id", p.id).WithField("backup id", msg.ReplicaId).Error("received view change request")

	if msg.NewViewId < p.viewId.Load() {
		log.WithField("request", msg.String()).WithField("current-view", p.viewId.Load()).Warn("Received view change request with old view")
		return
	}

	if p.node.GetLeaderForView(msg.NewViewId) != p.id {
		log.WithField("request", msg.String()).Warn("Received view change request but not leader for view")
		return
	}

	if _, ok := p.viewChanges[msg.NewViewId]; !ok {
		p.viewChanges[msg.NewViewId] = make(map[string]*pb.ViewChangeRequest)
	}
	p.viewChanges[msg.NewViewId][msg.ReplicaId] = msg
	log.WithField("backup id", msg.ReplicaId).WithField("len", len(p.viewChanges[msg.NewViewId])).WithField("2f+1", 2*p.config.F()+1).Error("applied view change message")

	if len(p.viewChanges[msg.NewViewId]) == 2*p.config.F()+1 {
		minSeq := p.store.GetLastStableCheckpoint().GetSequenceNumber()
		maxSeq := int64(0)

		// Determine minSeq and maxSeq from all ViewChange messages
		for _, viewChange := range p.viewChanges[msg.NewViewId] {
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
		for _, viewChange := range p.viewChanges[msg.NewViewId] {
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
				if len(replicaIDs) < 2*p.config.F() {
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
		viewChangeMessages := make([]*pb.ViewChangeRequest, 0, len(p.viewChanges[msg.NewViewId]))
		for _, vc := range p.viewChanges[msg.NewViewId] {
			viewChangeMessages = append(viewChangeMessages, vc)
		}

		// Broadcast NewView message
		newViewMessage := &pb.NewViewRequest{
			NewViewId:       msg.NewViewId,
			ViewChangeProof: viewChangeMessages,
			Preprepares:     preprepares,
			ReplicaId:       p.id,
		}
		p.sender.Broadcast("NewView", newViewMessage)
		log.WithField("my-id", p.id).Error("broadcasted new view message")

		// Update view changer state
		p.inViewChange = false
		p.viewId.Store(msg.NewViewId)
		p.viewTimer.Reset(p.requestTimeout)
		p.currentViewChangeTimeout = p.baseViewChangeTimeout
	}
}
