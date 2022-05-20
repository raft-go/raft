package raft

// Command 一致性模型需要提交, 状态机需要处理的命令
type Command []byte

// CommandLogEntry
// used for state machine to apply
type CommandLogEntry struct {
	Index uint64
	Term  uint64

	Command Command
}
