package raft

import (
	"encoding/json"
	"sync"
)

// configManager manage cluster configs
type configManager interface {
	// GetPeers get a deep copy of cluster config peers
	GetPeersList() [][]RaftPeer
	// GetIndex get last cluster config's log entry index
	GetIndex() uint64
	// LenConfigs configs length
	LenConfigs() int
	// AddConfig add cluster config
	AddConfig(index uint64, peers []RaftPeer) error
	// FallbackConfig fall back to previous cluster config
	FallbackConfig() error
	// AdvanceConfig go forward cluster config
	AdvanceConfig() error
}

func newConfigManager(store Store) (*configManagerImpl, error) {
	m := &configManagerImpl{
		configsKey: []byte("raft.configs.key"),
		store:      store,
	}
	err := m.load()
	if err != nil {
		return nil, err
	}
	return m, nil
}

var _ configManager = (*configManagerImpl)(nil)

// configManagerImpl implement configManager
type configManagerImpl struct {
	mux        sync.RWMutex
	configs    []config
	configsKey []byte

	store Store
}

func (m *configManagerImpl) load() error {
	b, err := m.store.Get(m.configsKey)
	if err != nil {
		return err
	}
	return m.unmarshal(b, &m.configs)
}

// GetPeersList get a deep copy of cluster config peers
func (m *configManagerImpl) GetPeersList() [][]RaftPeer {
	m.mux.RLock()
	defer m.mux.RUnlock()

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

// LenConfigs configs length
func (m *configManagerImpl) LenConfigs() int {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return len(m.configs)
}

// AddConfig add cluster config
func (m *configManagerImpl) AddConfig(index uint64, peers []RaftPeer) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.configs = append(m.configs, config{
		index: index,
		peers: peers,
	})

	b, err := m.marshal(m.configs)
	if err != nil {
		return err
	}
	return m.store.Set(m.configsKey, b)
}

// FallbackConfig fall back to previous cluster config
func (m *configManagerImpl) FallbackConfig() error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if len(m.configs) == 0 {
		return nil
	}

	m.configs = m.configs[:len(m.configs)-1]

	b, err := m.marshal(m.configs)
	if err != nil {
		return err
	}
	return m.store.Set(m.configsKey, b)
}

// AdvanceConfig go forward cluster config
func (m *configManagerImpl) AdvanceConfig() error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if len(m.configs) == 0 {
		return nil
	}

	b, err := m.marshal(m.configs)
	if err != nil {
		return err
	}
	return m.store.Set(m.configsKey, b)
}

func (*configManagerImpl) marshal(configs []config) ([]byte, error) {
	return json.Marshal(configs)
}

func (*configManagerImpl) unmarshal(b []byte, configs *[]config) error {
	return json.Unmarshal(b, configs)
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

// peersList2Peers flaten peersList and unique
func peersList2Peers(peersList [][]RaftPeer) []RaftPeer {
	var outPeers []RaftPeer
	for _, peers := range peersList {
		for _, peer := range peers {
			var has bool
			for j := range outPeers {
				if outPeers[j].Id == peer.Id {
					has = true
					break
				}
			}
			if !has {
				outPeers = append(outPeers, peer)
			}
		}
	}
	return outPeers
}

func newDecider(peersList [][]RaftPeer) *decider {
	return &decider{
		peersList: peersList,
		counts:    make([]int, len(peersList)),
	}
}

// decider 决策器
type decider struct {
	peersList [][]RaftPeer
	counts    []int
}

func (d *decider) AddVote(voterId RaftId) *decider {
	for i, peers := range d.peersList {
		for _, peer := range peers {
			if peer.Id == voterId {
				d.counts[i]++
				break
			}
		}
	}
	return d
}

func (d *decider) HasAchievedMajority() bool {
	achievedMajority := true
	for i, peers := range d.peersList {
		if d.counts[i] <= len(peers)/2 {
			achievedMajority = false
			break
		}
	}
	return achievedMajority
}

func (d *decider) Counts() []int {
	return d.counts
}
