package pbft

import (
	"fmt"
	pb "github.com/Arman17Babaei/pbft/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNode_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	viewChangeTimeout := 100 * time.Hour
	maxOutstandingRequests := 15
	checkpointInterval := 5
	waterMarkInterval := 20
	config := &Config{
		Id: "node_0",
		PeersAddress: map[string]*Address{
			"node_0": {Host: "localhost", Port: 3000},
			"node_1": {Host: "localhost", Port: 3001},
			"node_2": {Host: "localhost", Port: 3002},
			"node_3": {Host: "localhost", Port: 3003},
		},
		Timers: &Timers{
			ViewChangeTimeoutMs: int(viewChangeTimeout.Milliseconds()),
		},
		General: &General{
			EnabledByDefault:       true,
			MaxOutstandingRequests: maxOutstandingRequests,
			CheckpointInterval:     checkpointInterval,
			WaterMarkInterval:      waterMarkInterval,
		},
	}

	t.Run("initiate preprepare on request init", func(t *testing.T) {
		sender := NewMockISender(ctrl)
		_, requestCh, _, _, node := NewMockNode(sender, config)
		request := newRequest()
		preprepareRequest := &pb.PrePrepareRequest{
			ViewId:         0,
			SequenceNumber: 1,
			RequestDigest:  "",
		}
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("PrePrepare", gomock.Any()).
			Do(func(method string, message proto.Message) {
				preprepareMessage, ok := message.(*pb.PiggyBackedPrePareRequest)
				assert.True(t, ok)
				assert.ElementsMatch(t, []*pb.ClientRequest{request}, preprepareMessage.GetRequests())
				assert.NotNil(t, preprepareMessage.GetPrePrepareRequest())
				assert.EqualExportedValues(t, preprepareRequest, preprepareMessage.GetPrePrepareRequest())
			}).
			Times(1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()
		requestCh <- request
		time.Sleep(10 * time.Millisecond)
		node.Stop()
		wg.Wait()

		assert.Equal(t, node.Preprepares[1], preprepareRequest)
	})

	t.Run("initiate prepare on preprepare for backup", func(t *testing.T) {
		sender := NewMockISender(ctrl)
		inputCh, _, _, _, node := NewMockNode(sender, config)
		node.LeaderId = "" // no-one is leader
		request := newRequest()
		preprepareMessage := &pb.PiggyBackedPrePareRequest{
			PrePrepareRequest: &pb.PrePrepareRequest{
				ViewId:         0,
				SequenceNumber: 1,
				RequestDigest:  "",
			},
			Requests: []*pb.ClientRequest{request},
		}
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Prepare", gomock.Any()).
			Do(func(method string, message proto.Message) {
				prepareMessage, ok := message.(*pb.PrepareRequest)
				assert.True(t, ok)
				assert.EqualExportedValues(t, &pb.PrepareRequest{
					ViewId:         0,
					SequenceNumber: 1,
					RequestDigest:  "",
					ReplicaId:      config.Id,
				}, prepareMessage)
			}).
			Times(1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()
		inputCh <- preprepareMessage
		time.Sleep(10 * time.Millisecond)
		node.Stop()
		wg.Wait()
	})

	t.Run("initiate prepare on preprepare for leader", func(t *testing.T) {
		sender := NewMockISender(ctrl)
		inputCh, _, _, _, node := NewMockNode(sender, config)
		node.LeaderId = config.Id // is leader
		request := newRequest()
		preprepareMessage := &pb.PiggyBackedPrePareRequest{
			PrePrepareRequest: &pb.PrePrepareRequest{
				ViewId:         0,
				SequenceNumber: 1,
				RequestDigest:  "",
			},
			Requests: []*pb.ClientRequest{request},
		}
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Prepare", gomock.Any()).Times(0)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()
		inputCh <- preprepareMessage
		time.Sleep(10 * time.Millisecond)
		node.Stop()
		wg.Wait()
	})

	t.Run("initiate commit on prepare for backup", func(t *testing.T) {
		sender := NewMockISender(ctrl)
		inputCh, _, _, _, node := NewMockNode(sender, config)
		node.LeaderId = "" // no-one is leader
		request := newRequest()
		preprepareMessage := &pb.PiggyBackedPrePareRequest{
			PrePrepareRequest: &pb.PrePrepareRequest{
				ViewId:         0,
				SequenceNumber: 1,
				RequestDigest:  "",
			},
			Requests: []*pb.ClientRequest{request},
		}
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Prepare", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Commit", gomock.Any()).
			Do(func(method string, message proto.Message) {
				commitMessage, ok := message.(*pb.CommitRequest)
				assert.True(t, ok)
				assert.EqualExportedValues(t, newCommit(config.Id, 1), commitMessage)
			}).
			Times(1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()
		inputCh <- preprepareMessage
		inputCh <- newPrepare("node_1", 1)
		inputCh <- newPrepare("node_2", 1)
		time.Sleep(10 * time.Millisecond)
		node.Stop()
		wg.Wait()
	})

	t.Run("initiate checkpoint on commit for backup", func(t *testing.T) {
		sender := NewMockISender(ctrl)
		inputCh, _, _, _, node := NewMockNode(sender, config)
		node.LeaderId = "" // no-one is leader
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Prepare", gomock.Any()).Times(checkpointInterval)
		sender.EXPECT().Broadcast("Commit", gomock.Any()).Times(checkpointInterval)
		sender.EXPECT().SendRPCToClient(gomock.Any(), "Response", gomock.Any()).Times(checkpointInterval)
		sender.EXPECT().Broadcast("Checkpoint", gomock.Any()).Times(1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()

		for i := checkpointInterval; i > 0; i-- {
			inputCh <- preprpareRequest(i)
		}
		for i := checkpointInterval; i > 0; i-- {
			inputCh <- newPrepare("node_1", i)
			inputCh <- newPrepare("node_2", i)
		}
		for i := checkpointInterval; i > 0; i-- {
			inputCh <- newCommit("node_1", i)
			inputCh <- newCommit("node_2", i)
		}

		time.Sleep(20 * time.Millisecond)
		node.Stop()
		wg.Wait()
	})

	t.Run("stabilizing checkpoint", func(t *testing.T) {
		sender := NewMockISender(ctrl)
		inputCh, _, _, _, node := NewMockNode(sender, config)
		node.LeaderId = "" // no-one is leader
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Prepare", gomock.Any()).Times(checkpointInterval)
		sender.EXPECT().Broadcast("Commit", gomock.Any()).Times(checkpointInterval)
		sender.EXPECT().SendRPCToClient(gomock.Any(), "Response", gomock.Any()).Times(checkpointInterval)
		sender.EXPECT().Broadcast("Checkpoint", gomock.Any()).Times(1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()

		for i := checkpointInterval; i > 0; i-- {
			inputCh <- preprpareRequest(i)
		}
		for i := checkpointInterval; i > 0; i-- {
			inputCh <- newPrepare("node_1", i)
			inputCh <- newPrepare("node_2", i)
		}
		for i := checkpointInterval; i > 0; i-- {
			inputCh <- newCommit("node_1", i)
			inputCh <- newCommit("node_2", i)
		}
		inputCh <- newCheckpoint("node_1", checkpointInterval)
		inputCh <- newCheckpoint("node_2", checkpointInterval)

		time.Sleep(20 * time.Millisecond)
		node.Stop()
		wg.Wait()

		assert.Equal(t, checkpointInterval, int(node.Store.GetLastStableSequenceNumber()))
	})

	t.Run("stabilizing many checkpoints", func(t *testing.T) {
		activeNodes := []string{"node_1", "node_2", "node_3"}
		numCheckpoints := 100
		numTransactions := numCheckpoints * checkpointInterval

		sender := NewMockISender(ctrl)
		inputCh, _, _, _, node := NewMockNode(sender, config)
		node.LeaderId = "" // no-one is leader
		sender.EXPECT().Broadcast("GetStatus", gomock.Any()).Times(1)
		sender.EXPECT().Broadcast("Prepare", gomock.Any()).Times(numTransactions)
		sender.EXPECT().Broadcast("Commit", gomock.Any()).Times(numTransactions)
		sender.EXPECT().SendRPCToClient(gomock.Any(), "Response", gomock.Any()).Times(numTransactions)
		sender.EXPECT().Broadcast("Checkpoint", gomock.Any()).Times(numCheckpoints)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			node.Run()
			wg.Done()
		}()

		messages := make([]proto.Message, 0, 7*numTransactions+numCheckpoints)
		var mu sync.Mutex
		var messagesWg sync.WaitGroup
		messagesString := make([]string, 0, 7*numTransactions+numCheckpoints)
		for i := range numTransactions {
			txnId := i + 1
			messagesWg.Add(1)
			go func(txnId int) {
				time.Sleep(1 * time.Millisecond)
				mu.Lock()
				messages = append(messages, preprpareRequest(txnId))
				messagesString = append(messagesString, fmt.Sprintf("preprpareRequest(%d)", txnId))
				mu.Unlock()
				for _, id := range activeNodes {
					messagesWg.Add(1)
					go func() {
						time.Sleep(1 * time.Millisecond)
						mu.Lock()
						messages = append(messages, newPrepare(id, txnId))
						messagesString = append(messagesString, fmt.Sprintf("newPrepare(\"%s\", %d)", id, txnId))
						mu.Unlock()

						time.Sleep(1 * time.Millisecond)
						mu.Lock()
						messages = append(messages, newCommit(id, txnId))
						messagesString = append(messagesString, fmt.Sprintf("newCommit(\"%s\", %d)", id, txnId))
						mu.Unlock()

						if txnId%checkpointInterval == 0 {
							time.Sleep(1 * time.Millisecond)
							mu.Lock()
							messages = append(messages, newCheckpoint(id, txnId))
							messagesString = append(messagesString, fmt.Sprintf("newCheckpoint(\"%s\", %d)", id, txnId))
							mu.Unlock()
						}

						messagesWg.Done()
					}()
				}

				messagesWg.Done()
			}(txnId)
			time.Sleep(1 * time.Millisecond)
		}
		messagesWg.Wait()

		for _, message := range messages {
			inputCh <- message
		}

		time.Sleep(20 * time.Millisecond)
		node.Stop()
		wg.Wait()

		fmt.Println(strings.Join(messagesString, ", "))

		assert.Equal(t, numTransactions, int(node.Store.GetLastStableSequenceNumber()))
	})
}

func newCheckpoint(id string, seqNo int) *pb.CheckpointRequest {
	return &pb.CheckpointRequest{
		SequenceNumber: int64(seqNo),
		StateDigest:    []byte("0"),
		ReplicaId:      id,
		ViewId:         0,
	}
}

func newCommit(id string, seqNo int) *pb.CommitRequest {
	return &pb.CommitRequest{
		ViewId:         0,
		SequenceNumber: int64(seqNo),
		RequestDigest:  "",
		ReplicaId:      id,
	}
}

func preprpareRequest(seqNo int) *pb.PiggyBackedPrePareRequest {
	request := newRequest()
	preprepareMessage := &pb.PiggyBackedPrePareRequest{
		PrePrepareRequest: &pb.PrePrepareRequest{
			ViewId:         0,
			SequenceNumber: int64(seqNo),
			RequestDigest:  "",
		},
		Requests: []*pb.ClientRequest{request},
	}
	return preprepareMessage
}

func newPrepare(id string, seqNo int) *pb.PrepareRequest {
	return &pb.PrepareRequest{
		ViewId:         0,
		SequenceNumber: int64(seqNo),
		RequestDigest:  "",
		ReplicaId:      id,
	}
}

func newRequest() *pb.ClientRequest {
	return &pb.ClientRequest{
		ClientId:    "client_id",
		TimestampMs: time.Now().UnixMilli(),
		Operation:   &pb.Operation{Type: pb.Operation_GET},
		Callback:    "callback",
	}
}

func NewMockNode(sender ISender, config *Config) (chan proto.Message, chan *pb.ClientRequest, chan any, chan any, *Node) {
	inputCh := make(chan proto.Message, 5)
	requestCh := make(chan *pb.ClientRequest, 5)
	enableCh := make(chan any)
	disableCh := make(chan any)
	return inputCh, requestCh, enableCh, disableCh, NewNode(config, sender, inputCh, requestCh, enableCh, disableCh)
}
