package raft

import "sync"

func newState(store Store) (*state_, error) {
	s := &state_{
		store: store,

		keyCurrentTerm: []byte("state.CurrentTerm"),
		keyVotedFor:    []byte("state.VotedFor"),
	}
	err := s.loadCurrentTerm()
	if err != nil {
		return nil, err
	}
	err = s.loadVotedFor()
	if err != nil {
		return nil, err
	}

	return s, nil
}

// state raft state
type state interface {
	// ------------------------------------------------------
	// Persistent state on all servers:
	// 	(Updated on stable storage before responding to RPCs)
	// ------------------------------------------------------

	// CurrentIndex
	//	latest term server has seen (initialized to 0
	//	on first boot, increases monotonically)
	GetCurrentTerm() uint64
	SetCurrentTerm(uint64) error

	// VotedFor
	// 	candidateId that received vote in current term (or null if none)
	GetVotedFor() RaftId
	SetVotedFor(RaftId) error

	// ------------------------------------------------------
	// Volatile state on all servers:
	// ------------------------------------------------------

	// CommitIndex
	// 	index of highest log entry known to be
	// 	committed (initialized to 0, increases monotonically)
	GetCommitIndex() uint64
	SetCommitIndex(uint64)
	// LastApplied
	// 	index of highest log entry applied to state machine
	//  (initialized to 0, increases monotonically)
	GetLastApplied() uint64
	SetLastApplied(uint64)
}

var _ state = (*state_)(nil)

type state_ struct {
	mu sync.Mutex

	store Store

	keyCurrentTerm []byte
	keyVotedFor    []byte

	currentTerm uint64
	votedFor    RaftId

	commitIndex uint64
	lastApplied uint64
}

func (s *state_) loadCurrentTerm() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	term, err := s.store.GetUint64(s.keyCurrentTerm)
	if err != nil {
		return err
	}

	s.currentTerm = term
	return nil
}

func (s *state_) loadVotedFor() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, err := s.store.Get(s.keyVotedFor)
	if err != nil {
		return err
	}

	if len(value) == 0 {
		return nil
	}

	votedFor := RaftId(value)
	s.votedFor = votedFor
	return nil
}

func (s *state_) GetCurrentTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.currentTerm
}

func (s *state_) SetCurrentTerm(term uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentTerm >= term {
		return nil
	}
	s.currentTerm = term
	return s.store.SetUint64(s.keyCurrentTerm, term)
}

func (s *state_) GetVotedFor() RaftId {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.votedFor
}

func (s *state_) SetVotedFor(votedFor RaftId) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.votedFor = votedFor
	return s.store.Set(s.keyVotedFor, []byte(votedFor))
}

func (s *state_) GetCommitIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.commitIndex
}

func (s *state_) SetCommitIndex(i uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.commitIndex >= i {
		return
	}
	s.commitIndex = i
}

func (s *state_) GetLastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.lastApplied
}

func (s *state_) SetLastApplied(i uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastApplied >= i {
		return
	}
	s.lastApplied = i
}
