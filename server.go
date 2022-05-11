package raft

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrIsNotLeader = errors.New("err: raft consensuse module isn't at Leader state")
)

type serverAccessor interface {
	GetServer() server
	SetServer(server)
}

func newServerAccessor(mu *sync.Mutex) *serverValue {
	return &serverValue{
		mu: mu,
	}
}

var _ serverAccessor = (*serverValue)(nil)

type serverValue struct {
	mu *sync.Mutex

	server server
}

func (a *serverValue) GetServer() server {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.server
}

func (a *serverValue) SetServer(s server) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.server = s
}

// server raft 一致性模型在各个状态(Follower/Candidate/Leader)下的行为
type server interface {
	// Run 启动, 若发生状态转换会返回转换后的服务
	Run() (server, error)
	// 处理命令
	// append cmd --> 日志复制 --> 日志应用
	Handle(ctx context.Context, cmd ...Command) error
	// 返回服务的状态信息: Follower/Candidate/Leader
	String() string
	// 重置计时器
	// Follower/Candidate 重置选集计时器
	//  Leader 重置心跳计时器
	ResetTimer()
	// 是否是 Leader
	IsLeader() bool
	// AddPeers add peers to cluster
	AddPeers(peers []RaftPeer) error
	// RemovePeers remove peers from cluster
	RemovePeers(peers []RaftPeer) error
}
