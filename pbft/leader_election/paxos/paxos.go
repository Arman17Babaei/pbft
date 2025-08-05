package paxos

import (
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"google.golang.org/protobuf/proto"
)

type Node interface {
	GetLastStableCheckPoint()
}

type PaxosElection struct {
	config *configs.Config
	node   Node
	sender *Sender
	// 你可以根据需要添加更多状态字段
}

func NewPaxosElection(config *configs.Config, node Node, sender *Sender) *PaxosElection {
	return &PaxosElection{
		config: config,
		node:   node,
		sender: sender,
	}
}

// 实现接口方法
func (p *PaxosElection) GetLeader() string {
	// 返回当前已选出的leader
	return "" // TODO: 实现
}

func (p *PaxosElection) FindLeaderForView(viewId int64, callbackCh chan string) {
	// TODO: 实现Paxos选主流程
}

func (p *PaxosElection) Start() error {
	// 启动Paxos选举服务
	return nil
}

func (p *PaxosElection) Stop() error {
	// 停止Paxos选举服务
	return nil
}

func (p *PaxosElection) HandleMessage(msg proto.Message) error {
	// 处理Paxos相关消息
	return nil
}

func (p *PaxosElection) GetCurrentLeader() string {
	// 返回当前leader
	return ""
}

func (p *PaxosElection) IsLeader() bool {
	// 判断自己是否是leader
	return false
}
