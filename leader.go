package raft

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var _ server = (*leader)(nil)

// leader 实现一致性模型在 Leader 状态下的行为
type leader struct {
	*raft

	//	for each server, index of the next log entry
	//	to send to that server (initialized to leader
	//	last log index + 1)
	nextIndex raftIdIndexMap
	//	for each server, index of highest log entry
	//	known to be replicated on server (initialized to 0,
	//	increases monotonically)
	matchIndex raftIdIndexMap

	// once resetTimer
	once sync.Once

	// cluster configuration commit cond
	configCommitCond *sync.Cond
}

func (l *leader) Run() (server, error) {
	// Upon election: send initial empty AppendEntries RPC
	// (heartbeat) to each server
	err := l.sendHeartbeats()
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-l.Done():
			return nil, ErrStopped
		case args := <-l.rpcArgs:
			server, converted, err := l.reactToRPCArgs(args)
			if err != nil {
				return nil, err
			}
			if converted {
				return server, nil
			}
		case <-l.ticker.C:
			// repeat during idle periods to
			// prevent election timeouts (§5.2)
			err := l.sendHeartbeats()
			if err != nil {
				return nil, err
			}
		}
	}
}

// Handle
// append entry to local log,
// respond after entry applied to state machine (§5.3)
//
// append log entry -->  log replication --> apply 客户端命令 cmd
func (l *leader) Handle(ctx context.Context, cmd ...Command) error {
	if len(cmd) == 0 {
		return nil
	}

	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	entries := make([]LogEntry, 0, len(cmd))
	currentTerm := l.GetCurrentTerm()
	for i := range cmd {
		entries = append(entries, LogEntry{
			Term:    currentTerm,
			Command: cmd[i],
		})
	}
	err := l.Append(entries...)
	if err != nil {
		return err
	}

	err = l.replicate(ctx)
	if err != nil {
		return err
	}
	ok, err := l.refreshCommitIndex()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	return l.applyCommitted()
}

func (l *leader) sendHeartbeats() error {
	// Leaders send periodic
	// heartbeats (AppendEntries RPCs that carry no log entries)
	// to all followers in order to maintain their authority.
	var wg sync.WaitGroup
	peers := l.config.Peers()
	for i := range peers {
		peer := peers[i]
		id, addr := peer.Id, peer.Addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Id() == id {
				return
			}
			// empty args
			var args = AppendEntriesArgs{
				Term:     l.GetCurrentTerm(),
				LeaderId: l.Id(),
			}
			l.rpc.CallAppendEntries(addr, args)
		}()
	}
	wg.Wait()
	return nil
}

// ResetTimer
// 重置计时器(心跳)
func (l *leader) ResetTimer() {
	// leader 状态只需要重置一次定时器
	// 接受到其他节点的请求, 无需重置定时器
	l.once.Do(func() {
		timeout := l.heartbeatTimeout()
		l.ticker.Reset(timeout)
	})
}

func (*leader) String() string {
	return "Leader"
}

// replicate
// replicate log entries
func (l *leader) replicate(ctx context.Context) error {
	peers := l.config.Peers()
	replicateCh := make(chan struct{}, len(peers))

	go func() {
		defer close(replicateCh)

		var wg sync.WaitGroup
		for i := range peers {
			peer := peers[i]
			id, addr := peer.Id, peer.Addr
			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					select {
					case <-ctx.Done():
						return
					default:
						// no-op
					}

					if l.Id() == id {
						lastLogIndex, _, err := l.Last()
						if err != nil {
							continue
						}
						netxIndex := lastLogIndex + 1
						l.nextIndex.Store(id, netxIndex)
						matchIndex := lastLogIndex
						l.matchIndex.Store(id, matchIndex)
						replicateCh <- struct{}{}
						return
					}

					ok, err := l.replicateTo(id, addr)
					if err != nil {
						continue
					}
					if ok {
						replicateCh <- struct{}{}
					}
				}
			}()
		}
		wg.Wait()
	}()

	var count int
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-replicateCh:
			count++
			if count > len(peers)/2 {
				return nil
			}
		}
	}
}

// replicateTo replacate log entries to peer
//
// TODO: should control message size per replicate
func (l *leader) replicateTo(id RaftId, addr RaftAddr) (ok bool, err error) {
	nextIndex, _ := l.nextIndex.Load(id)
	prevLogIndex := nextIndex - 1
	prevLogTerm, err := l.Get(prevLogIndex)
	if err != nil {
		return false, err
	}

	var entries []LogEntry
	// 为了避免 Figure 8 的问题
	// 若最新 log entry 的 term 不是 currentTerm
	// 则不复制
	lastLogIndex, lastLogTerm, err := l.Last()
	if err != nil {
		return false, err
	}
	if lastLogTerm == l.GetCurrentTerm() {
		// FIXME: 什么时候会出现 last log index < next ?
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		if lastLogIndex >= nextIndex {
			start, end := nextIndex-1, lastLogIndex
			entries, err = l.RangeGet(start, end)
			if err != nil {
				return false, err
			}
		}
	}

	args := AppendEntriesArgs{
		Term:         l.GetCurrentTerm(),
		LeaderId:     l.Id(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: l.GetCommitIndex(),
	}

	results, err := l.rpc.CallAppendEntries(addr, args)
	if err != nil {
		l.debug("Call %s's AppendEntries, err: %+v", id, err)
		return false, err
	}

	// If successful: update nextIndex and matchIndex for
	// follower (§5.3)
	if results.Success {
		if len(args.Entries) > 0 {
			nextIndex := args.Entries[len(args.Entries)-1].Index + 1
			l.nextIndex.Store(id, nextIndex)
		}
		l.matchIndex.Store(id, prevLogIndex+uint64(len(args.Entries)))
		return true, nil
	}

	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	if nextIndex == 1 {
		return false, nil
	}
	l.nextIndex.Store(id, nextIndex-1)

	return results.Success, nil
}

// refreshCommitIndex
//
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (l *leader) refreshCommitIndex() (bool, error) {
	l.commitCond.L.Lock()
	defer l.commitCond.L.Unlock()

	// Raft never commits log entries from previous terms by count-
	// ing replicas. Only log entries from the leader’s current
	// term are committed by counting replicas; once an entry
	// from the current term has been committed in this way,
	// then all prior entries are committed indirectly because
	// of the Log Matching Property. There are some situations
	// where a leader could safely conclude that an older log en-
	// try is committed (for example, if that entry is stored on ev-
	// ery server), but Raft takes a more conservative approach
	// for simplicity
	_, lastLogTerm, err := l.Last()
	if err != nil {
		return false, err
	}
	if lastLogTerm != l.GetCurrentTerm() {
		return false, nil
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	var matchIndex []uint64
	l.matchIndex.Range(func(_ RaftId, index uint64) bool {
		matchIndex = append(matchIndex, index)
		return true
	})
	commitIndex := l.GetCommitIndex()
	sort.Sort(uint64Slice(matchIndex))
	mid := (len(matchIndex) - 1) / 2
	nextCommitIndex := matchIndex[mid]

	if nextCommitIndex <= commitIndex {
		return false, nil
	}
	nextTerm, err := l.Get(nextCommitIndex)
	if err != nil {
		return false, err
	}
	if nextTerm != l.GetCurrentTerm() {
		return false, nil
	}
	l.SetCommitIndex(nextCommitIndex)

	// signal cluster configuration has been committed
	configIndex := l.config.LogIndex()
	if configIndex > commitIndex && configIndex <= nextCommitIndex {
		l.configCommitCond.Signal()
	}

	return true, nil
}

func (*leader) IsLeader() bool {
	return true
}

var (
	ErrRaftIdHasBeenUsed = errors.New("err: raft id has been used by another raft server")
)

// AddServer
// Invoked by admin to add a server to cluster configuration
func (l *leader) AddServer(args AddServerArgs, results *AddServerResults) error {
	defer func() {
		results.LeaderHints = l.addr
	}()

	if args.NewId.isNil() {
		return ErrInvalidRaftId
	}
	peer, ok := l.config.Peers().getById(args.NewId)
	if ok {
		if peer.Addr != args.NewServer {
			return ErrRaftIdHasBeenUsed
		}
		// wait for an election timout
		time.Sleep(l.electionTimeout[1])
		// it should has been added
		index := l.config.LogIndex()
		lastLogIndex, _, err := l.Log.Last()
		if err != nil {
			return err
		}
		if lastLogIndex >= index {
			results.SetOK()
			return nil
		}
		results.Status = "not ok, you can try again later"
		return nil
	}

	// Catch up new server for fixed number of rounds, Reply TIMEOUT
	// if new server does not make progress for an election timeout or
	// if the last round takes longer than the election timeout(&4.2.1)
	const round = 10
	for i := 1; i <= round; i++ {
		start := time.Now()
		ok, err := l.replicateTo(args.NewId, args.NewServer)
		if i != round {
			continue
		}

		if err != nil {
			return err
		}
		if !ok || time.Since(start) > l.electionTimeout[0] {
			results.Status = "not ok, new server may bee too slow"
			return nil
		}
	}

	// Wait until previous configuration in log is committed(&4.1)
	l.configCommitCond.L.Lock()
	for l.config.LogIndex() > l.GetCommitIndex() {
		l.configCommitCond.Wait()
	}
	l.configCommitCond.L.Unlock()

	// Append new configuration entry to log(old configuration plus newServer),
	peers := l.config.Peers()
	peers = append(peers, raftPeer{args.NewId, args.NewServer})
	configEntry := LogEntry{
		Term:    l.GetCurrentTerm(),
		Type:    logEntryTypeClusterConfiguration,
		Command: peers2Command(peers),
	}
	index, err := l.AppendEntry(configEntry)
	if err != nil {
		return err
	}
	err = l.config.Use(index, peers)
	if err != nil {
		return err
	}

	// commit it using majority of new configuration(&4.1)
	err = l.replicate(context.Background())
	if err != nil {
		return err
	}
	_, err = l.refreshCommitIndex()
	if err != nil {
		return err
	}

	// Reply OK
	results.SetOK()

	return nil
}

// RemoveServer
// invoked by admin to remove a server to cluster configuration
//
// implementation:
// 1. Reply NOT_LEADER if not leader(&6.2)
// 2. Wait until previous configuration in log is committed(&4.1)
// 3. Append new configuration entry to log(old configuration without oldServer),
//	commit it using majority of new configuration (&4.1)
// 4. Reply OK and, if this server was removed, step down(&4.2.2)
func (l *leader) RemoveServer(args RemoveServerArgs, results *RemoveServerResults) error {
	// TODO:
	return nil
}

type nextIndexMap raftIdIndexMap

type matchIndexMap raftIdIndexMap

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

// uint64Slice attaches the methods of Interface to []uint64, sorting in increasing order.
type uint64Slice []uint64

func (x uint64Slice) Len() int           { return len(x) }
func (x uint64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x uint64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
