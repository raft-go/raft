package raft

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Log raft log
type Log interface {
	// Get 获取 raft log 中索引为 index 的 log entry term
	// 若无, 则返回 0, nil
	Get(index uint64) (term uint64, err error)
	// Match 是否有匹配上 term 与 index 的 log entry
	Match(index, term uint64) (bool, error)
	// Last 返回最后一个 log entry 的 term 与 index
	// 若无, 则返回 0 , 0
	Last() (index, term uint64, err error)
	// RangeGet 获取在 (i, j] 索引区间的 log entry
	// 若无, 则返回 nil, nil
	RangeGet(i, j uint64) ([]LogEntry, error)
	// AppendAfter 在afterIndex之后追加 log entry
	AppendAfter(afterIndex uint64, entries ...LogEntry) error
	// Append 追加log entry
	Append(entries ...LogEntry) error
	// AppendEntry 追加一个 log entry , 并返回索引
	AppendEntry(entry LogEntry) (index uint64, err error)

	// DequeueTo dequeue log entry to index
	// and retain index and coressponding term(
	// allows the AppendEntries consistency check to continue to work)
	DequeueTo(index uint64) error
	// LastDequeueTo get last dequeue to log entry index and term
	LastDequeueTo() (index, term uint64)
}

const (
	logEntryTypeCommand = LogEntryType_command
	logEntryTypeConfig  = LogEntryType_config
)

// Entry raft log entry
//	each entry contains command for state machine,
//	and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Index      uint64
	Term       uint64
	Type       LogEntryType
	Command    Command
	AppendTime time.Time
}

var _ Log = (*memoryLog)(nil)

// memoryLog just for testing
type memoryLog struct {
	mux                               sync.Mutex
	queue                             []LogEntry
	lastDequeueIndex, lastDequeueTerm uint64
}

// Get 获取 raft log 中索引为 index 的 log entry term
// 若无, 则返回 0, nil
func (l *memoryLog) Get(index uint64) (term uint64, err error) {
	l.mux.Lock()
	defer l.mux.Unlock()

	if index == 0 {
		return 0, nil
	}

	index--

	length := uint64(len(l.queue))
	if index >= 0 && index < length {
		entry := l.queue[index]
		return entry.Term, nil
	}
	return 0, nil
}

// Match 是否有匹配上 term 与 index 的 log entry
func (l *memoryLog) Match(index, term uint64) (bool, error) {
	l.mux.Lock()
	defer l.mux.Unlock()
	if index == 0 {
		return true, nil
	}

	index--
	length := uint64(len(l.queue))
	if length <= index {
		return false, nil
	}
	entry := l.queue[index]
	target := entry.Term
	return term == target, nil
}

// Last 返回最后一个 log entry 的 term 与 index
// 若无, 则返回 0 , 0
func (l *memoryLog) Last() (index, term uint64, err error) {
	l.mux.Lock()
	defer l.mux.Unlock()

	return l.last()
}

func (l *memoryLog) last() (index, term uint64, err error) {
	if len(l.queue) == 0 {
		return 0, 0, nil
	}

	entry := l.queue[len(l.queue)-1]
	return entry.Index, entry.Term, nil
}

// RangeGet 获取在 (i, j] 索引区间的 log entry
// 若无, 则返回 nil, nil
func (l *memoryLog) RangeGet(i, j uint64) ([]LogEntry, error) {
	l.mux.Lock()
	defer l.mux.Unlock()

	if j <= i {
		return nil, nil
	}

	i--
	j--
	var entries []LogEntry
	for k := i + 1; k <= j && k < uint64(len(l.queue)); k++ {
		entry := l.queue[k]
		entries = append(entries, entry)
	}
	return entries, nil
}

// AppendAfter 追加 log entry
func (l *memoryLog) AppendAfter(afterIndex uint64, entries ...LogEntry) error {
	l.mux.Lock()
	defer l.mux.Unlock()

	// pop after
	if afterIndex > uint64(len(l.queue)) {
		msg := fmt.Sprintf("afterIndex(%d) out of range", afterIndex)
		return errors.New(msg)
	}
	l.queue = l.queue[:afterIndex]

	// append
	start := afterIndex + 1
	for i := range entries {
		entries[i].Index = start + uint64(i)
	}
	l.queue = append(l.queue, entries...)
	return nil
}

// Append 追加log entry
func (l *memoryLog) Append(entries ...LogEntry) error {
	l.mux.Lock()
	defer l.mux.Unlock()

	last, _, err := l.last()
	if err != nil {
		return err
	}
	start := last + 1
	for i := range entries {
		entries[i].Index = start + uint64(i)
		l.queue = append(l.queue, entries[i])
	}
	return nil
}

// AppendEntry 追加一个 log entry , 并返回索引
func (l *memoryLog) AppendEntry(entry LogEntry) (index uint64, err error) {
	l.mux.Lock()
	defer l.mux.Unlock()
	last, _, err := l.last()
	if err != nil {
		return 0, err
	}

	entry.Index = last + 1
	l.queue = append(l.queue, entry)
	return entry.Index, nil
}

// DequeueTo dequeue log entry to index
func (l *memoryLog) DequeueTo(index uint64) error {
	l.mux.Lock()
	defer l.mux.Unlock()

	if len(l.queue) < int(index) {
		return nil
	}

	l.lastDequeueIndex = index
	entry := l.queue[index-1]
	l.lastDequeueTerm = entry.Term

	if len(l.queue) == int(index) {
		l.queue = nil
	} else {
		l.queue = l.queue[index:]
	}
	return nil
}

func (l *memoryLog) LastDequeueTo() (index, term uint64) {
	l.mux.Lock()
	defer l.mux.Unlock()

	return l.lastDequeueIndex, l.lastDequeueTerm
}
