package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// RaftPeer raft peer
type RaftPeer struct {
	Id   RaftId
	Addr RaftAddr
}

func (p RaftPeer) String() string {
	return fmt.Sprintf("(%s, %s)", p.Id, p.Addr)
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

// configManager manage cluster configs
type configManager interface {
	// GetConfig()
	GetConfig() config
	// UseConfig use config cfg
	UseConfig(cfg config) error
	// FallbackConfig fall back to previous cluster config
	FallbackConfig() error

	// NewConfigLogEntry
	NewConfigLogEntry(term uint64, cfg config) (*LogEntry, error)
	// NewConfig
	NewConfig(index uint64, data []byte) (config, error)
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

// GetConfig
func (m *configManagerImpl) GetConfig() config {
	m.mux.RLock()
	defer m.mux.RUnlock()
	return m.getConfig()
}

func (m *configManagerImpl) getConfig() config {
	if len(m.configs) == 0 {
		return zeroConfig
	}
	return m.configs[len(m.configs)-1]
}

// UseConfig use config cfg
func (m *configManagerImpl) UseConfig(cfg config) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if cfg.GetIndex() <= m.getConfig().GetIndex() {
		return errors.New("prepare to use config's index is less than or equal current config")
	}
	m.configs = append(m.configs, cfg)
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

// NewConfigLogEntry
func (*configManagerImpl) NewConfigLogEntry(term uint64, cfg config) (*LogEntry, error) {
	b, err := cfg.Bytes()
	if err != nil {
		return nil, err
	}
	return &LogEntry{
		Term:    term,
		Type:    logEntryTypeConfig,
		Command: b,
	}, nil
}

// NewConfig
func (*configManagerImpl) NewConfig(index uint64, peersListBytes []byte) (config, error) {
	var config configImpl
	err := json.Unmarshal(peersListBytes, &config.peersList)
	if err != nil {
		return nil, err
	}
	config.index = index
	return &config, err
}

func (*configManagerImpl) marshal(configs []config) ([]byte, error) {
	return json.Marshal(configs)
}

func (*configManagerImpl) unmarshal(b []byte, configs *[]config) error {
	return json.Unmarshal(b, configs)
}

// config cluster configuration
type config interface {
	// IsJoint 是否是 joint consensus config
	IsJoint() bool
	// GetIndex 获取配置对应的 log entry index
	GetIndex() uint64
	// GetPeers 获取集群配置所有的 peer
	GetPeers() []RaftPeer
	// NewDecider 生成该配置的决策器
	NewDecider() decider
	// GenJointConfig 根据 add peers 与 remove peers 生成 joint consensus configuration
	GenJointConfig(add []RaftPeer, remove []RaftId) config
	// SetIndex set i to config' log entry index
	// 只能设置一次
	SetIndex(i uint64)
	// Bytes
	Bytes() ([]byte, error)
	// NewCommitCalc
	NewCommitCalc() commitCalc
	// CreateNewConfig
	CreateNewConfig() (config, error)
}

var zeroConfig config = &configImpl{}

var _ config = (*configImpl)(nil)

// configImpl implement config interface
type configImpl struct {
	index     uint64
	peersList [][]RaftPeer

	once sync.Once
}

// IsJoint 是否是 joint consensus config
func (c *configImpl) IsJoint() bool {
	return len(c.peersList) > 1
}

// GetIndex 获取配置对应的 log entry index
func (c *configImpl) GetIndex() uint64 {
	return c.index
}

// GetPeers 获取集群配置所有的 peer
func (c *configImpl) GetPeers() []RaftPeer {
	var result []RaftPeer
	for _, peers := range c.peersList {
		for _, peer := range peers {
			if !includePeer(result, peer) {
				result = append(result, peer)
			}
		}
	}
	return result
}

// NewDecider 生成该配置的决策器
func (c *configImpl) NewDecider() decider {
	return &deciderImpl{
		peersList: c.peersList,
		counts:    make([]int, len(c.peersList)),
	}
}

// NewCommitCalc
func (c *configImpl) NewCommitCalc() commitCalc {
	return &commitCalcImpl{
		peersList:  c.peersList,
		matchIndex: make(map[RaftId]uint64),
	}
}

// GenJointConfig 根据 add peers 与 remove peers 生成 joint consensus configuration
func (c *configImpl) GenJointConfig(add []RaftPeer, remove []RaftId) config {
	length := len(c.peersList)
	peers := clonePeers(c.peersList[length-1])
	for _, peer := range add {
		if !includePeer(peers, peer) {
			peers = append(peers, peer)
		}
	}
	for _, id := range remove {
		i := 0
		for i < len(peers) {
			if peers[i].Id == id {
				peers[i] = peers[len(peers)-1]
				peers = peers[:len(peers)-1]
			} else {
				i++
			}
		}
	}
	return nil
}

// SetIndex set i to config' log entry index
// 只能设置一次
func (c *configImpl) SetIndex(i uint64) {
	if c.index > 0 || i < 1 {
		return
	}
	c.index = i
}

// Bytes
func (c *configImpl) Bytes() ([]byte, error) {
	return json.Marshal(c.peersList)
}

// CreateNewConfig
func (c *configImpl) CreateNewConfig() (config, error) {
	if !c.IsJoint() {
		msg := "isn't joint config, can not create C(new)"
		return nil, errors.New(msg)
	}

	length := len(c.peersList)
	peers := clonePeers(c.peersList[length-1])
	config := &configImpl{
		peersList: [][]RaftPeer{peers},
	}
	return config, nil
}

// includePeer peers 中是否包含 peer
func includePeer(peers []RaftPeer, peer RaftPeer) bool {
	for i := range peers {
		if peers[i].Id == peer.Id {
			return true
		}
	}
	return false
}

// clonePeers deep clone peers
func clonePeers(peers []RaftPeer) []RaftPeer {
	results := make([]RaftPeer, 0, len(peers))
	for i := range peers {
		results = append(results, peers[i])
	}
	return results
}

// decider
type decider interface {
	AddVote(voterId RaftId)
	HasAchievedMajority() bool
	Counts() []int
}

var _ decider = (*deciderImpl)(nil)

// deciderImpl implement decider
type deciderImpl struct {
	peersList [][]RaftPeer
	counts    []int
}

func (d *deciderImpl) AddVote(voterId RaftId) {
	for i, peers := range d.peersList {
		for _, peer := range peers {
			if peer.Id == voterId {
				d.counts[i]++
				break
			}
		}
	}
}

func (d *deciderImpl) HasAchievedMajority() bool {
	if len(d.peersList) == 0 {
		return false
	}

	achievedMajority := true
	for i, peers := range d.peersList {
		if d.counts[i] <= len(peers)/2 {
			achievedMajority = false
			break
		}
	}
	return achievedMajority
}

func (d *deciderImpl) Counts() []int {
	return d.counts
}

// commitCalc 根据每个 peer 的 matchIndex
// 计算下一个 commitIndex
type commitCalc interface {
	Add(id RaftId, matchIndex uint64)
	Calc() (nextCommitIndex uint64)
}

type commitCalcImpl struct {
	peersList  [][]RaftPeer
	matchIndex map[RaftId]uint64
}

func (c *commitCalcImpl) Add(id RaftId, matchIndex uint64) {
	if len(c.peersList) == 0 {
		return
	}

	c.matchIndex[id] = matchIndex
}

func (c *commitCalcImpl) Calc() (nextCommitIndex uint64) {
	if len(c.peersList) == 0 {
		return 0
	}

	nextCommitIndexes := make([]uint64, 0, len(c.peersList))
	for _, peers := range c.peersList {
		nextCommitIndexes = append(nextCommitIndexes, c.calcFor(peers))
	}

	nextCommitIndex = nextCommitIndexes[0]
	for i := 1; i < len(nextCommitIndexes); i++ {
		if nextCommitIndexes[i] < nextCommitIndex {
			nextCommitIndex = nextCommitIndexes[i]
		}
	}
	return nextCommitIndex
}

func (c *commitCalcImpl) calcFor(peers []RaftPeer) uint64 {
	if len(peers) == 0 {
		return 0
	}
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	matchIndex := make([]uint64, 0, len(peers))
	for _, peer := range peers {
		matchIndex = append(matchIndex, c.matchIndex[peer.Id])
	}
	sort.Sort(uint64Slice(matchIndex))
	mid := (len(matchIndex) - 1) / 2
	return matchIndex[mid]
}
