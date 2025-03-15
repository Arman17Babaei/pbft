package pbft

import (
	"slices"
	"strconv"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
)

type CheckpointProof struct {
	proof []*pb.CheckpointRequest
}

type CheckpointId struct {
	sequenceNumber int64
	digest         string
}

type State struct {
	value map[string]int
}

type Store struct {
	config                    *Config
	unstableCheckpoints       map[CheckpointId]*CheckpointProof
	lastStableCheckpoint      *CheckpointProof
	state                     *State
	requests                  map[int64][]*pb.ClientRequest
	lastAppliedSequenceNumber int64
	committedRequests         map[int64][]*pb.ClientRequest
}

func NewStore(config *Config) *Store {
	return &Store{
		config:                    config,
		unstableCheckpoints:       make(map[CheckpointId]*CheckpointProof),
		lastStableCheckpoint:      nil,
		lastAppliedSequenceNumber: 0,
		committedRequests:         make(map[int64][]*pb.ClientRequest),
		state:                     &State{value: make(map[string]int)},
		requests:                  make(map[int64][]*pb.ClientRequest),
	}
}

func NewCheckpointId(checkpoint *pb.CheckpointRequest) CheckpointId {
	return CheckpointId{
		sequenceNumber: checkpoint.SequenceNumber,
		digest:         string(checkpoint.StateDigest),
	}
}

func (s *Store) GetLastStableSequenceNumber() int64 {
	if s.lastStableCheckpoint == nil {
		return 0
	}
	return s.lastStableCheckpoint.proof[0].SequenceNumber
}

func (s *Store) GetLastStableCheckpoint() []*pb.CheckpointRequest {
	if s.lastStableCheckpoint == nil {
		return []*pb.CheckpointRequest{}
	}

	return s.lastStableCheckpoint.proof
}

func (s *Store) UpdateLastStableCheckpoint(checkpointProof []*pb.CheckpointRequest) {
	s.lastStableCheckpoint = &CheckpointProof{proof: checkpointProof}
}

func (s *Store) AddCheckpointRequest(checkpoint *pb.CheckpointRequest) *int64 {
	log.WithField("checkpoint", checkpoint.String()).Debug("adding checkpoint request to store")

	if checkpoint.SequenceNumber < s.GetLastStableSequenceNumber() {
		log.WithField("checkpoint", checkpoint.String()).Warn("stale checkpoint")
		return nil
	}

	id := NewCheckpointId(checkpoint)
	if _, ok := s.unstableCheckpoints[id]; !ok {
		s.unstableCheckpoints[id] = &CheckpointProof{proof: []*pb.CheckpointRequest{}}
	}

	s.unstableCheckpoints[id].proof = append(s.unstableCheckpoints[id].proof, checkpoint)
	if len(s.unstableCheckpoints[id].proof) == 2*s.config.F()+1 {
		sequenceNumber := &s.unstableCheckpoints[id].proof[0].SequenceNumber
		s.stabilizeCheckpoint(s.unstableCheckpoints[id])
		return sequenceNumber
	}

	return nil
}

func (s *Store) AddRequests(sequenceNumber int64, reqs []*pb.ClientRequest) {
	log.WithField("sequence-number", sequenceNumber).Debug("adding requests to store")
	s.requests[sequenceNumber] = reqs
}

func (s *Store) Commit(commit *pb.CommitRequest) ([]*pb.ClientRequest, []*pb.OperationResult, []*pb.CheckpointRequest) {
	log.WithField("request", commit.String()).Debug("committing request")
	//log.WithField("request", commit.GetSequenceNumber()).WithField("replica", s.config.Id).Error("committing request")

	s.committedRequests[commit.SequenceNumber] = s.requests[commit.SequenceNumber]

	var reqs []*pb.ClientRequest
	var results []*pb.OperationResult
	var checkpoints []*pb.CheckpointRequest

	for ; s.committedRequests[s.lastAppliedSequenceNumber+1] != nil; s.lastAppliedSequenceNumber++ {
		requests := s.requests[s.lastAppliedSequenceNumber+1]
		reqs = append(reqs, requests...)

		for _, req := range requests {
			results = append(results, s.state.apply(req.Operation))
		}

		if int(s.lastAppliedSequenceNumber+1)%s.config.General.CheckpointInterval == 0 {
			log.WithField("seq-no", s.lastAppliedSequenceNumber+1).WithField("replica", s.config.Id).Error("created checkpoint for seq-no")
			checkpoints = append(checkpoints, &pb.CheckpointRequest{
				SequenceNumber: s.lastAppliedSequenceNumber + 1,
				StateDigest:    []byte(s.state.Digest()),
				ReplicaId:      s.config.Id,
			})
		}
	}

	return reqs, results, checkpoints
}

func (s *State) Digest() string {
	return strconv.Itoa(s.value[""])
}

func (s *Store) stabilizeCheckpoint(checkpoint *CheckpointProof) {
	log.WithField("checkpoint", checkpoint.proof[0].SequenceNumber).Debug("stabilizing checkpoint")
	s.lastStableCheckpoint = checkpoint

	// remove stale checkpoints
	for id, cp := range s.unstableCheckpoints {
		if cp.proof[0].SequenceNumber <= checkpoint.proof[0].SequenceNumber {
			delete(s.unstableCheckpoints, id)
		}
	}

	// remove stale requests
	for seqNo := range s.requests {
		if seqNo <= checkpoint.proof[0].SequenceNumber {
			delete(s.requests, seqNo)
		}
	}

	// remove unapplied requests
	haveStaleState := false
	for seqNo := range s.committedRequests {
		if seqNo <= checkpoint.proof[0].SequenceNumber {
			delete(s.committedRequests, seqNo)
			haveStaleState = true
		}
	}

	if haveStaleState {
		s.state = &State{value: make(map[string]int)}
		// In reality this should be a call to other services to update missing requests
		// Since it can be done outside the critical path, we can ignore it here
		s.state.apply(&pb.Operation{Type: pb.Operation_ADD, Key: "", Value: string(checkpoint.proof[0].StateDigest)})
	}
}

func (s *State) apply(operation *pb.Operation) *pb.OperationResult {
	value := 0
	if slices.Contains([]pb.Operation_Type{pb.Operation_ADD, pb.Operation_SUB}, operation.Type) {
		var err error
		value, err = strconv.Atoi(operation.Value)

		if err != nil {
			log.WithField("request", operation.String()).Error("failed to convert value to int")
			return &pb.OperationResult{
				Value: strconv.Itoa(s.value[operation.Key]),
			}
		}
	}

	switch operation.Type {
	case pb.Operation_GET:
		// do nothing
	case pb.Operation_ADD:
		s.value[operation.Key] += value
	case pb.Operation_SUB:
		s.value[operation.Key] -= value
	default:
		log.WithField("operation", operation.String()).Error("unknown operation type")
	}

	return &pb.OperationResult{
		Value: strconv.Itoa(s.value[operation.Key]),
	}
}
