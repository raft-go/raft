package raft

import (
	"fmt"
	"sync"
)

// raftIdIndexMap used for leader's matchIndex and nextIndex
type raftIdIndexMap struct {
	mux sync.Mutex
	m   map[RaftId]uint64
}

func (m *raftIdIndexMap) Load(id RaftId) (index uint64, ok bool) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.m == nil {
		m.m = map[RaftId]uint64{}
	}

	index, ok = m.m[id]
	return index, ok
}

func (m *raftIdIndexMap) Store(id RaftId, index uint64) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.m == nil {
		m.m = map[RaftId]uint64{}
	}

	m.m[id] = index
}

func (m *raftIdIndexMap) Range(fn func(id RaftId, index uint64) bool) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.m == nil {
		m.m = map[RaftId]uint64{}
	}

	for id, index := range m.m {
		ok := fn(id, index)
		if !ok {
			return
		}
	}
}

// neaten remove unused index
func (m *raftIdIndexMap) neaten(usedPeers []RaftPeer) {
	m.mux.Lock()
	defer m.mux.Unlock()

	for id := range m.m {
		if !includePeer(usedPeers, RaftPeer{Id: id}) {
			delete(m.m, id)
		}
	}
}

// String
func (m *raftIdIndexMap) String() string {
	m.mux.Lock()
	defer m.mux.Unlock()
	return fmt.Sprintf("%+v", m.m)
}
