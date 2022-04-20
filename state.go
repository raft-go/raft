package raft

// state raft state
//
// Persistent state on all servers:
// 	(Updated on stable storage before responding to RPCs)
//
// 	currentTerm:
//				latest term server has seen (initialized to 0
//				on first boot, increases monotonically)
// 	votedFor:
//				candidateId that received vote in current term (or null if none)
// 	log[]:
//				log entries; each entry contains command for state machine,
//				and term when entry was received by leader (first index is 1)
//
// Volatile state on all servers:
//
// 	commitIndex:
//					index of highest log entry known to be
// 					committed (initialized to 0, increases monotonically)
// 	lastApplied:
//					index of highest log entry applied to state
// 					machine (initialized to 0, increases monotonically)
type state interface {
	Load() error

	// CurrentIndex
	//	latest term server has seen (initialized to 0
	//	on first boot, increases monotonically)
	GetCurrentTerm() int
	SetCurrentTerm(int)

	// candidateId that received vote in current term (or null if none)
	GetVotedFor() *RaftId
	SetVotedFor(*RaftId)

	log

	// CommitIndex
	// 	index of highest log entry known to be
	// 	committed (initialized to 0, increases monotonically)
	GetCommitIndex() int
	SetCommitIndex(int)

	// LastApplied
	// 	index of highest log entry applied to state machine
	//  (initialized to 0, increases monotonically)
	GetLastApplied() int
	SetLastApplied(int)
}
