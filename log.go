package raft

import (
	"errors"
)

var (
	ErrLogEntryNotExists = errors.New("err: raft log entry not exits")
)

// log raft log
// 	log entries;
type log interface {
	// Len 获取 raft log 长度
	Len() int
	// Get 获取 raft log 中索引为 i 的 log entry
	Get(i int) (LogEntry, error)
	// Match 是否有匹配上 term 与 index 的 log entry
	Match(index, term int) bool
	// Last 返回最后一个 log entry 的 term 与 index
	// 若无, 则返回 0 , 0
	Last() (index, term int)
	// RangeGet 获取在 (i, j] 索引区间的 log entry
	RangeGet(i, j int) ([]LogEntry, error)
	// PopAfter 删除索引 i 之后的所有 log entry
	PopAfter(i int) error
	// Append 追加 log entry
	Append(entries ...LogEntry) error
}

// LogEntry raft log entry
//	each entry contains command for state machine,
//	and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Index   int
	Term    int
	Command Command
}
