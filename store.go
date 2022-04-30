package raft

import (
	"encoding/binary"
	"sync"
)

// Store is used to provide stable storage
// of key configurations to ensure safety.
type Store interface {
	Set(key []byte, val []byte) error
	// Get returns the value for key, or an empty byte slice if key was not found.
	Get(key []byte) ([]byte, error)

	SetUint64(key []byte, val uint64) error
	// GetUint64 returns the uint64 value for key, or 0 if key was not found.
	GetUint64(key []byte) (uint64, error)
}

var _ Store = (*memoryStore)(nil)

// memoryStore just for testing
type memoryStore struct {
	mux sync.Mutex
	m   map[string]string
}

func (s *memoryStore) Set(key []byte, val []byte) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.m == nil {
		s.m = make(map[string]string)
	}

	s.m[string(key)] = string(val)
	return nil
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *memoryStore) Get(key []byte) ([]byte, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.m == nil {
		s.m = make(map[string]string)
	}

	value, ok := s.m[string(key)]
	if !ok {
		return []byte{}, nil
	}
	return []byte(value), nil
}

func (s *memoryStore) SetUint64(key []byte, val uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, val)

	return s.Set(key, value)
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *memoryStore) GetUint64(key []byte) (uint64, error) {
	value, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(value) == 0 {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}
