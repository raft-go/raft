package raft

// Command 一致性模型需要提交, 状态机需要处理的命令
type Command []byte

// Commands 状态机需依序处理的命令
type Commands interface {
	// 告知已处理成功
	Ack() error
	// 获取命令序列
	Data() []Command
}

func newCommands(data []Command, ack func() error) *commands {
	return &commands{
		data: data,
		ack:  ack,
	}
}

var _ Commands = (*commands)(nil)

// commands 实现 Commands
type commands struct {
	data []Command
	ack  func() error
}

func (c *commands) Data() []Command {
	return c.data
}

func (c *commands) Ack() error {
	return c.ack()
}
