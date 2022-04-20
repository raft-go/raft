package raft

import "sync"

var _ server = (*leader)(nil)

// leader 实现一致性模型在 Leader 状态下的行为
//
// Volatile state on leaders:
// (Reinitialized after election)
//
// 	nextIndex[]:
//					for each server, index of the next log entry
// 					to send to that server (initialized to leader
//					last log index + 1)
// 	matchIndex[]:
//					for each server, index of highest log entry
// 					known to be replicated on server (initialized to 0,
//					increases monotonically)
type leader struct {
	*raft

	nextIndex  []int
	matchIndex []int

	once sync.Once
}

func (c *leader) Run() (server, error) {
	c.ResetTimer()
	// TODO:

	return nil, nil
}

func (c *leader) Commit(...Command) error {
	// TODO:
	return nil
}

func (c *leader) ResetTimer() {
	// leader 状态只需要重置一次定时器
	// 接受到其他节点的请求, 无需重置定时器
	c.once.Do(func() {
		timeout := c.HeartbeatTimout()
		c.ticker.Reset(timeout)
	})
}

func (c *leader) String() string {
	return "Leader"
}
