package raft

import (
	"encoding/json"
	"sync"
)

// configStore use to manage cluster configurations
type configStore interface {
	// Peers get a deep copy of last cluster configuration peers
	Peers() []raftPeer
	// LogIndex get the log entry index of last cluster configuration
	LogIndex() uint64
	// Use logIndex and peers as latest cluster configuration
	Use(logIndex uint64, peers []raftPeer) error
	// FallBack fall back to the previous cluster configuration
	FallBack() error
}

// newConfigStore
func newConfigStore(store Store) configStore {
	return &configManager{
		store:      store,
		keyConfigs: []byte("raft.configs"),
	}
}

var _ configStore = (*configManager)(nil)

// configManager
// used to manage and persist cluster configurations
type configManager struct {
	mux sync.RWMutex

	configs    configs
	keyConfigs []byte

	store Store
}

// Peers return a deep copy of last cluster configuration peers
func (m *configManager) Peers() []raftPeer {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return m.configs.Last().Clone().Peers
}

// LogIndex returns the last cluster configuration's log entry index
func (m *configManager) LogIndex() uint64 {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return m.configs.Last().LogIndex
}

// Use logIndex and peers as latest cluster configuration
func (m *configManager) Use(logIndex uint64, peers []raftPeer) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	configs := m.configs.Clone()
	configs.Use(logIndex, peers)
	value, err := m.marshal(configs)
	if err != nil {
		return err
	}
	key := m.keyConfigs
	err = m.store.Set(key, value)
	if err != nil {
		return err
	}

	m.configs = configs
	return nil
}

// FallBack fall back to the previous configuration
func (m *configManager) FallBack() error {
	m.mux.Lock()
	defer m.mux.Unlock()

	configs := m.configs.Clone()
	configs.FallBack()
	value, err := m.marshal(configs)
	if err != nil {
		return err
	}
	key := m.keyConfigs
	err = m.store.Set(key, value)
	if err != nil {
		return err
	}

	m.configs = configs
	return nil
}

func (m *configManager) marshal(configs configs) ([]byte, error) {
	return json.Marshal(configs)
}

func (m *configManager) unmarshal(b []byte, configs *configs) error {
	return json.Unmarshal(b, configs)
}

// configs cluster configurations
// used to manage cluster configuration in memory
type configs struct {
	// last cluster configuration's index at buf field
	last int
	buf  [2]clusterConfig
}

func (c *configs) Last() clusterConfig {
	return c.buf[c.last]
}

// Use use logIndex and peers as last cluster config
func (c *configs) Use(logIndex uint64, peers []raftPeer) {
	c.last = 1 - c.last
	c.buf[c.last].LogIndex = logIndex
	c.buf[c.last].Peers = peers
}

// FallBack fall back to the previous cluster configuration
func (c *configs) FallBack() {
	var config clusterConfig
	c.buf[c.last] = config
	c.last = 1 - c.last
}

func (c *configs) Clone() configs {
	var clone configs
	clone.last = c.last
	for i := range c.buf {
		clone.buf[i] = c.buf[i].Clone()
	}
	return clone
}

// clusterConfig
// the set of servers participating in the consensus algorithm
type clusterConfig struct {
	// the log entry index of the cluster configuration log entry
	LogIndex uint64
	Peers    []raftPeer
}

func (c clusterConfig) Clone() clusterConfig {
	var cc clusterConfig
	cc.LogIndex = c.LogIndex
	cc.Peers = append(cc.Peers, c.Peers...)
	return cc
}

func peers2Command(peers []raftPeer) Command {
	cmd, err := json.Marshal(peers)
	if err != nil {
		panic(err)
	}
	return cmd
}

func command2Peers(command Command) (peers []raftPeer, err error) {
	err = json.Unmarshal(command, &peers)
	if err != nil {
		return nil, err
	}
	return peers, nil
}

// raftPeer  a single raft server infomation
type raftPeer struct {
	Id   RaftId
	Addr RaftAddr
}
