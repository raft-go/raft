package raft

import "sync"

// StableStore is used to provide stable storage
// of key configurations to ensure safety.
type StableStore interface {
	Set(key []byte, val []byte) error

	// Get returns the value for key, or an empty byte slice if key was not found.
	Get(key []byte) ([]byte, error)

	SetInt(key []byte, val int) error

	// GetInt returns the int value for key, or 0 if key was not found.
	GetInt(key []byte) (int, error)
}

func newState(store StableStore) (*state_, error) {
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
	GetCurrentTerm() int
	SetCurrentTerm(int) error

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
	GetCommitIndex() int
	SetCommitIndex(int)
	// LastApplied
	// 	index of highest log entry applied to state machine
	//  (initialized to 0, increases monotonically)
	GetLastApplied() int
	SetLastApplied(int)
}

var _ state = (*state_)(nil)

type state_ struct {
	mu sync.Mutex

	store StableStore

	keyCurrentTerm []byte
	keyVotedFor    []byte

	currentTerm int
	votedFor    RaftId

	commitIndex int
	lastApplied int
}

func (s *state_) loadCurrentTerm() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	term, err := s.store.GetInt(s.keyCurrentTerm)
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

func (s *state_) GetCurrentTerm() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.currentTerm
}

func (s *state_) SetCurrentTerm(term int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentTerm = term
	return s.store.SetInt(s.keyCurrentTerm, term)
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

func (s *state_) GetCommitIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.currentTerm
}

func (s *state_) SetCommitIndex(i int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commitIndex = i
}

func (s *state_) GetLastApplied() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.lastApplied
}

func (s *state_) SetLastApplied(i int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastApplied = i
}
