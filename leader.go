package raft

import (
	"context"
	logger "log"
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
	entries := make([]LogEntry, len(cmd))
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

	return l.ApplyCommitted()
}

func (l *leader) sendHeartbeats() error {
	timeout := l.HeartbeatTimout() / 2
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := l.replicate(ctx)
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
	l.commitCond.Signal()
	return nil
}

// ResetTimer
// 重置计时器(心跳)
func (l *leader) ResetTimer() {
	// leader 状态只需要重置一次定时器
	// 接受到其他节点的请求, 无需重置定时器
	l.once.Do(func() {
		timeout := l.HeartbeatTimout()
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
						logger.Printf("get last entry, err : %+v", err)
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
								logger.Printf("RangeGet(%d, %d), err: %+v", start, end, err)
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

					results, err := l.client.CallAppendEntries(addr, args)
					if err != nil {
						logger.Printf("call append entries, addr: %q, err: %+v", addr, err)
						return
					}

					// If successful: update nextIndex and matchIndex for
					// follower (§5.3)
					if results.Success {
						if len(args.Entries) > 0 {
							nextIndex := args.Entries[len(args.Entries)-1].Index + 1
							l.nextIndex.Store(id, nextIndex)
						}
						l.matchIndex.Store(id, prevLogIndex)
						replicateCh <- struct{}{}
						return
					}
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (§5.3)
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
	matchIndex := make([]int, 0)
	l.matchIndex.Range(func(_ RaftId, index int) bool {
		matchIndex = append(matchIndex, index)
		return true
	})
	commitIndex := l.GetCommitIndex()
	matchIndex = append(matchIndex, commitIndex)

	sort.Ints(matchIndex)
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

type raftIdIndexMap struct {
	m sync.Map
}

func (m *raftIdIndexMap) Load(id RaftId) (index int, ok bool) {
	value, ok := m.m.Load(id)
	if !ok {
		return 0, false
	}
	return value.(int), true
}

func (m *raftIdIndexMap) Store(id RaftId, index int) {
	m.m.Store(id, index)
}

func (m *raftIdIndexMap) Range(fn func(id RaftId, index int) bool) {
	m.m.Range(func(key, value any) bool {
		id := key.(RaftId)
		index := value.(int)
		return fn(id, index)
	})
}
