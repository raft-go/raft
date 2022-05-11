package raft

import "sync"

// configManager manage cluster configs
type configManager interface {
	// GetPeers get a deep copy of cluster config peers
	GetPeers() [][]RaftPeer
	// GetIndex get last cluster config's log entry index
	GetIndex() uint64
	// AddConfig add cluster config
	AddConfig(index uint64, peers []RaftPeer)
	// FallbackConfig fall back to previous cluster config
	FallbackConfig()
	// AdvanceConfig go forward cluster config
	AdvanceConfig()
}

func newConfigManager() *configManagerImpl {
	return &configManagerImpl{}
}

var _ configManager = (*configManagerImpl)(nil)

// configManagerImpl implement configManager
type configManagerImpl struct {
	mux     sync.RWMutex
	configs []config
}

// GetPeers get a deep copy of cluster config peers
func (m *configManagerImpl) GetPeers() [][]RaftPeer {
	m.mux.RLock()
	defer m.mux.Unlock()

	var peersList [][]RaftPeer
	for i := range m.configs {
		config := m.configs[i]
		peers := raftPeers(config.peers).Clone()
		peersList = append(peersList, peers)
	}
	return peersList
}

// GetIndex get last cluster config's log entry index
func (m *configManagerImpl) GetIndex() uint64 {
	m.mux.RLock()
	defer m.mux.RUnlock()

	if len(m.configs) == 0 {
		return 0
	}
	return m.configs[0].index
}

// AddConfig add cluster config
func (m *configManagerImpl) AddConfig(index uint64, peers []RaftPeer) {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.configs = append(m.configs, config{
		index: index,
		peers: peers,
	})
}

// FallbackConfig fall back to previous cluster config
func (m *configManagerImpl) FallbackConfig() {
	m.mux.Lock()
	defer m.mux.Unlock()

	if len(m.configs) == 0 {
		return
	}

	m.configs = m.configs[:len(m.configs)-1]
}

// AdvanceConfig go forward cluster config
func (m *configManagerImpl) AdvanceConfig() {
	m.mux.Lock()
	defer m.mux.Unlock()

	if len(m.configs) == 0 {
		return
	}

	m.configs = m.configs[1:]
}

type config struct {
	index uint64
	peers []RaftPeer
}

type raftPeers []RaftPeer

func (ps raftPeers) Clone() raftPeers {
	peers := make(raftPeers, 0, len(ps))
	for i := range ps {
		peers = append(peers, ps[i])
	}
	return peers
}

// RaftPeer raft peer
type RaftPeer struct {
	Id   RaftId
	Addr RaftAddr
}
