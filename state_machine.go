package raft

import "io"

// StateMachine state machine
type StateMachine interface {
	// Apply 依序应用 commands 到状态机中
	// 返回 应用的 Command 数量 appliedCount
	Apply(commands ...CommandLogEntry) (appliedCount int, err error)

	// WaitForLastPersisted
	// wait for last log entry's index
	// that has been persisted for snapshotted
	WaitForLastPersisted() (index uint64)

	// GetSnapShotReader
	// used to read snapshot and send to follower
	// whose log is far behind the leader’s
	GetSnapShotReader() (SnapshotReader, error)
	// GetSnapShotWriter
	// used to write snapshot when follower is receiving snap from leader
	GetSnapShotWriter() (SnapshotWriter, error)
}

// SnapshotReader raft snapshot reader
type SnapshotReader interface {
	io.ReadCloser

	// get last included index and term
	//
	// index: the snapshot replaces all entries up through and including this index
	// term: term of lastIncludedIndex
	GetLastIncluded() (index, term uint64)
}

// SnapshotWriter raft snapshot writer
type SnapshotWriter interface {
	io.WriteCloser

	// set last included index and term
	//
	// index: the snapshot replaces all entries up through and including this index
	// term: term of lastIncludedIndex
	SetLastIncluded(index, term uint64) error
	// Cancel
	Cancel() error
}
