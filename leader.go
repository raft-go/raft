package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	// noop for send no-op entry just once
	noop int32

	// ccm configuration changes mutext
	ccm *sync.Mutex

	// jointCommitCond
	jointCommitCond *sync.Cond

	// stepDown wether or not been stepped down
	stepDown int32
}

func (l *leader) Run() (server, error) {
	// Upon election: sendding initial empty AppendEntries RPC
	// (heartbeat) to each server
	err := l.sendHeartbeats()
	if err != nil {
		return nil, err
	}
	// wait to transite to C(new)
	done := make(chan struct{})
	defer close(done)
	go l.loopTransiteToNewConfig(done)

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
			// the leader steps down (returns to follower state)
			if atomic.LoadInt32(&l.stepDown) != 0 {
				l.debug("Stepped down, convert to follower...")
				return l.toFollower(l.GetCurrentTerm())
			}

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

	err = l.replicateToAll(ctx)
	if err != nil {
		return err
	}
	ok, err := l.refreshCommitIndex()
	if err != nil {
		return err
	}
	if !ok {
		panic("refresh commit index failed")
	}

	return l.applyCommitted()
}

func (l *leader) sendHeartbeats() error {
	// Leaders send periodic
	// heartbeats (AppendEntries RPCs that carry no log entries)
	// to all followers in order to maintain their authority.
	var wg sync.WaitGroup
	config := l.raft.configs.GetConfig()
	for _, peer := range config.GetPeers() {
		id, addr := peer.Id, peer.Addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			if id == l.Id() {
				l.refreshLastHeartbeat()
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

// replicateToAll
// replicateToAll log entries to all peers
func (l *leader) replicateToAll(ctx context.Context) error {
	config := l.configs.GetConfig()
	peers := config.GetPeers()
	replicateCh := make(chan RaftId, len(peers))

	go func() {
		defer close(replicateCh)

		var wg sync.WaitGroup
		for _, peer := range peers {
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

					success, err := l.replicate(id, addr)
					if err != nil {
						continue
					}
					if success {
						replicateCh <- id
						return
					}
				}
			}(peer.Id, peer.Addr)
		}
		wg.Wait()
	}()

	decider := config.NewDecider()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case replicateId := <-replicateCh:
			decider.AddVote(replicateId)
			if decider.HasAchievedMajority() {
				return nil
			}
		}
	}
}

// replicate replicate log entries to specify peer
func (l *leader) replicate(id RaftId, addr RaftAddr) (success bool, err error) {
	lastLogIndex, _, err := l.Last()
	if err != nil {
		return false, err
	}
	if l.Id() == id {
		netxIndex := lastLogIndex + 1
		l.nextIndex.Store(id, netxIndex)
		matchIndex := lastLogIndex
		l.matchIndex.Store(id, matchIndex)
		return true, nil
	}

	nextIndex, ok := l.nextIndex.Load(id)
	if !ok {
		nextIndex = lastLogIndex + 1
	}
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
		return results.Success, nil
	}

	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	if nextIndex == 1 {
		return results.Success, nil
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

	// uses the latest configuration to make decision
	calculator := l.configs.GetConfig().NewCommitCalc()
	l.matchIndex.Range(func(id RaftId, index uint64) bool {
		calculator.Add(id, index)
		return true
	})
	nextCommitIndex := calculator.Calc()

	commitIndex := l.GetCommitIndex()
	if nextCommitIndex <= commitIndex {
		return true, nil
	}

	nextTerm, err := l.Get(nextCommitIndex)
	if err != nil {
		return false, err
	}
	if nextTerm != l.GetCurrentTerm() {
		return false, nil
	}
	l.SetCommitIndex(nextCommitIndex)

	// Once Cold,new has been committed, neither Cold nor Cnew
	// can make decisions without approval of the other, and the
	// Leader Completeness Property ensures that only servers
	// with the Cold,new log entry can be elected as leader. It is
	// now safe for the leader to create a log entry describing
	// Cnew and replicate it to the cluster.
	config := l.raft.configs.GetConfig()
	if !config.IsJoint() {
		return true, nil
	}
	pre := commitIndex
	now := nextCommitIndex
	index := config.GetIndex()
	if index > pre && index <= now {
		l.jointCommitCond.Signal()
	}
	return true, nil
}

func (*leader) IsLeader() bool {
	return true
}

// ChangeConfig add peers to cluster
func (l *leader) ChangeConfig(ctx context.Context, add []RaftPeer, remove []RaftId) error {
	if len(add)+len(remove) == 0 {
		return nil
	}

	// non-voting phase
	err := l.tryCatchupLeader(ctx, add)
	if err != nil {
		return err
	}

	l.ccm.Lock()
	defer l.ccm.Unlock()

	// generate joint consensus configuration
	config := l.raft.configs.GetConfig()
	jointConfig := config.GenJointConfig(add, remove)
	// store the configuration for joint consensus as log entry
	logEntry, err := l.configs.NewConfigLogEntry(
		l.GetCurrentTerm(), jointConfig)
	if err != nil {
		return err
	}
	index, err := l.Log.AppendEntry(*logEntry)
	if err != nil {
		return err
	}
	jointConfig.SetIndex(index)
	// uses that configuration for all future decisions (a server
	// always uses the latest configuration in its log, regardless
	// of whether the entry is committed).
	err = l.configs.UseConfig(jointConfig)
	if err != nil {
		return err
	}
	l.debug("~> C(old,new): %s", jointConfig)
	// replicates log entry
	err = l.replicateToAll(ctx)
	if err != nil {
		return err
	}
	// commit index
	ok, err := l.refreshCommitIndex()
	if err != nil {
		return err
	}
	if !ok {
		// FIXME:
		panic("refresh commit index failed")
	}
	return nil
}

// tryCatchupLeader catch up leader's log entries
//
// In order to avoid availability gaps, Raft introduces an additional phase before the configuration
// change, in which a new server joins the cluster as a non-voting member. The leader replicates
// log entries to it, but it is not yet counted towards majorities for voting or commitment purposes.
// Once the new server has caught up with the rest of the cluster, the reconfiguration can proceed
func (l *leader) tryCatchupLeader(ctx context.Context, peers []RaftPeer) error {
	errCh := make(chan error, len(peers))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer close(errCh)

		var wg sync.WaitGroup
		for i := range peers {
			peer := peers[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				// We suggest the following algorithm to determine when a new server is sufficiently caught up
				// to add to the cluster. The replication of entries to the new server is split into rounds, as shown in
				// Figure 4.5. Each round replicates all the log entries present in the leader’s log at the start of the
				// round to the new server’s log. While it is replicating entries for its current round, new entries may
				// arrive at the leader; it will replicate these during the next round. As progress is made, the round
				// durations shrink in time. The algorithm waits a fixed number of rounds (such as 10). If the last
				// round lasts less than an election timeout, then the leader adds the new server to the cluster, under
				// the assumption that there are not enough unreplicated entries to create a significant availability gap.
				const rounds = 10
				for i := 0; i < rounds; i++ {
					select {
					case <-ctx.Done():
						return
					default:
						// no-op
					}

					start := time.Now()
					success, err := l.replicate(peer.Id, peer.Addr)
					if i < rounds-1 {
						continue
					}

					if err != nil {
						errCh <- err
						return
					}
					timout := time.Since(start) > l.raft.electionTimeout[0]
					if timout || !success {
						format := "Peer %s may bee too slow to catch up leader"
						msg := fmt.Sprintf(format, peer)
						err = errors.New(msg)
						errCh <- err
						return
					}
				}
			}()
		}
		wg.Wait()
	}()
	return <-errCh
}

// loopTransiteToNewConfig wait for transitting from C(old,new) to C(new)
func (l *leader) loopTransiteToNewConfig(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-l.Done():
			return
		default:
			// no-op
		}

		func() {
			l.jointCommitCond.L.Lock()
			defer l.jointCommitCond.L.Unlock()

			config := l.configs.GetConfig()
			for !config.IsJoint() {
				l.jointCommitCond.Wait()
				config = l.configs.GetConfig()
			}
			err := l.transiteToNewConfig()
			if err != nil {
				l.debug("transite to C(new) failed, err: %v", err)
			}
		}()
	}
}

// transiteToNewConfig transite to C(new)
func (l *leader) transiteToNewConfig() error {
	config := l.raft.configs.GetConfig()
	newConfig, err := config.CreateNewConfig()
	if err != nil {
		return err
	}

	logEntry, err := l.configs.NewConfigLogEntry(l.GetCurrentTerm(), newConfig)
	if err != nil {
		return err
	}
	index, err := l.Log.AppendEntry(*logEntry)
	if err != nil {
		return err
	}
	newConfig.SetIndex(index)
	err = l.raft.configs.UseConfig(newConfig)
	if err != nil {
		return err
	}
	err = l.replicateToAll(context.Background())
	if err != nil {
		return err
	}
	_, err = l.refreshCommitIndex()
	if err != nil {
		return err
	}

	l.debug("~> C(new): %s", newConfig)
	// if leader is not in the new configuration,
	// the leader steps down (returns to follower state)
	// once it has committed the Cnew log entry.
	if !newConfig.IncludePeer(l.Id()) {
		atomic.SwapInt32(&l.stepDown, 1)
		return nil
	}

	// remove unused index
	peers := newConfig.GetPeers()
	l.nextIndex.neaten(peers)
	l.matchIndex.neaten(peers)
	return nil
}
