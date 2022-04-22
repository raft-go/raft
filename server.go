package raft

import (
	"errors"
	"time"
)

var (
	ErrIsNotLeader = errors.New("err: raft consensuse module isn't at Leader state")
)

// server raft 一致性模型在各个状态(Follower/Candidate/Leader)下的行为
type server interface {
	// Run 启动, 若发生状态转换会返回转换后的服务
	Run() (server, error)
	// 提交命令cmd
	Commit(timeout time.Duration, cmd ...Command) error
	// 返回服务的状态信息: Follower/Candidate/Leader
	String() string
	// 重置计时器
	// Follower/Candidate 重置选集计时器
	//  Leader 重置心跳计时器
	ResetTimer()
}
