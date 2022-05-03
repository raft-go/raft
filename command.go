package raft

// Command 一致性模型需要提交, 状态机需要处理的命令
type Command []byte

// Commands 状态机需依序处理的命令
type Commands interface {
	// 获取命令序列
	Data() []Command
}

func newCommands(entries []LogEntry) *commands {
	var data = make([]Command, 0, len(entries))
	for i := range entries {
		if entries[i].Type != logEntryTypeCommand {
			continue
		}
		data = append(data, entries[i].Command)
	}
	return &commands{
		data: data,
	}
}

var _ Commands = (*commands)(nil)

// commands 实现 Commands
type commands struct {
	data []Command
}

func (c *commands) Data() []Command {
	return c.data
}
