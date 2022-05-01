package raft

import (
	"context"
	"sort"
	"sync"
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
	for _, addr := range l.peers {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			// empty args
			var args AppendEntriesArgs
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
	replicateCh := make(chan struct{}, len(l.peers))

	go func() {
		defer close(replicateCh)

		var wg sync.WaitGroup
		for id, addr := range l.peers {
			if l.Id() == id {
				replicateCh <- struct{}{}
				continue
			}

			wg.Add(1)
			go func(id RaftId, addr RaftAddr) {
				defer wg.Done()

				for {
					select {
					case <-ctx.Done():
						return
					default:
						// no-op
					}

					nextIndex, _ := l.nextIndex.Load(id)
					prevLogIndex := nextIndex - 1
					prevLogTerm, err := l.Get(prevLogIndex)
					if err != nil {
						return
					}

					var entries []LogEntry
					// 为了避免 Figure 8 的问题
					// 若最新 log entry 的 term 不是 currentTerm
					// 则不复制
					lastLogIndex, lastLogTerm, err := l.Last()
					if err != nil {
						continue
					}
					if lastLogTerm == l.GetCurrentTerm() {
						// FIXME: 什么时候会出现 last log index < next ?
						// If last log index ≥ nextIndex for a follower: send
						// AppendEntries RPC with log entries starting at nextIndex
						if lastLogIndex >= nextIndex {
							start, end := nextIndex-1, lastLogIndex
							entries, err = l.RangeGet(start, end)
							if err != nil {
								continue
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
						return
					}

					// If successful: update nextIndex and matchIndex for
					// follower (§5.3)
					if results.Success {
						if len(args.Entries) > 0 {
							nextIndex := args.Entries[len(args.Entries)-1].Index + 1
							l.nextIndex.Store(id, nextIndex)
						}
						l.matchIndex.Store(id, prevLogIndex+uint64(len(args.Entries)))
						replicateCh <- struct{}{}
						return
					}
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (§5.3)
					if nextIndex == 1 {
						return
					}
					l.nextIndex.Store(id, nextIndex-1)
				}
			}(id, addr)
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
			if count > len(l.peers)/2 {
				return nil
			}
		}
	}
}

// refreshCommitIndex
//
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (l *leader) refreshCommitIndex() (bool, error) {
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
	mid := len(matchIndex) / 2
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
	return true, nil
}

func (*leader) IsLeader() bool {
	return true
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
