package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	grpc "google.golang.org/grpc"
)

var (
	ErrStopped       = errors.New("err: raft consensus module has been stopped")
	ErrRanRepeatedly = errors.New("err: raft consensus module can not bee ran repeatedly")
)

// New 实例化一个 raft 一致性模型
func New(id RaftId, addr RaftAddr, sm StateMachine, store Store, log Log, optFns ...OptFn) (Raft, error) {
	opts := newOpts()
	for _, fn := range optFns {
		fn(opts)
	}

	state, err := newState(store)
	if err != nil {
		return nil, err
	}

	configs, err := newConfigManager(store)
	if err != nil {
		return nil, err
	}

	raft := &raft{
		id: id,

		state: state,
		Log:   log,

		serverAccessor: newServerAccessor(&sync.Mutex{}),

		sm: sm,

		addr: addr,

		commitCond: sync.NewCond(&sync.Mutex{}),
		rpcArgs:    make(chan rpcArgs),

		configs:         configs,
		electionTimeout: opts.election,

		logger: opts.logger,

		bootstrapAsLeader: opts.bootstrapAsLeader,

		done: make(chan struct{}),
	}
	err = raft.init()
	if err != nil {
		return nil, err
	}

	return raft, nil
}

// Raft raft 一致性模型
type Raft interface {
	// Id 获取 raft 一致性模型 id
	Id() RaftId
	// Addr 获取 raft 一致性模型 rpc addr
	Addr() RaftAddr
	// IsLeader 是否是 Leader
	IsLeader() bool

	// Run 启动 raft 一致性模型
	Run() error
	// Stop 停止 raft 一致性模型
	Stop()
	// Done 是否已经停止
	Done() <-chan struct{}

	// Handle 处理 cmd
	//
	// append log entry --> log replication --> apply to state matchine
	Handle(ctx context.Context, cmd ...Command) error

	// ChangeConfig add added and remove removed
	ChangeConfig(ctx context.Context, added []RaftPeer, removed []RaftId) error
}

// RaftId raft 一致性模型 id
type RaftId = string

// RaftAddr raft 一致性模型 rpc 通信地址
type RaftAddr = string

var _ (Raft) = (*raft)(nil)

// raft 实现 raft 一致性模型
type raft struct {
	id RaftId

	state
	Log

	sm StateMachine

	serverAccessor

	addr       RaftAddr
	rpcServer  *grpc.Server
	rpcClients *rpcClients

	// 通知 commitIndex 更新事件发生
	commitCond *sync.Cond

	// 存放 rpc rpcArgs, 方便执行以下操作:
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	rpcArgs chan rpcArgs

	// cluster configuration
	configs configManager
	// electionTimeout
	electionTimeout [2]time.Duration

	// ticker heartbeat/election timer
	ticker *time.Ticker

	// lastHeartbeat last heartbeat's unix time (the number of milliseconds)
	// help to show leaders' activity
	lastHeartbeat int64

	logger Logger

	// whether or not already ran
	ran int32

	// wether or not bootstrap as leader
	bootstrapAsLeader bool

	// 表示一致性模型是否已停用
	done chan struct{}
}

func (r *raft) init() (err error) {
	r.rpcClients = newRpcClients(r)
	r.rpcServer = grpc.NewServer()

	timeout := r.randomElectionTimeout()
	ticker := time.NewTicker(timeout)
	r.ticker = ticker

	if r.bootstrapAsLeader {
		lastIndex, _, err := r.Log.Last()
		if err != nil {
			return err
		}
		if lastIndex == 0 {
			// Instead, we recommend that the very first time a cluster is created,
			// one server is initialized with a configuration entry as the first entry in its log.
			// This configuration lists only that one server;
			// it alone forms a majority of its configuration,
			// so it can consider this configuration committed.
			//
			// Other servers from then on should be initialized with empty logs;
			// they are added to the cluster and learn of the current configuration
			// through the membership change mechanism.
			peer := RaftPeer{r.Id(), r.Addr()}
			config := newBootstrapAsLeaderConfig(peer)
			entry, err := r.configs.NewConfigLogEntry(
				r.GetCurrentTerm(), config)
			if err != nil {
				return err
			}
			index, err := r.Log.AppendEntry(*entry)
			if err != nil {
				return err
			}
			config.SetIndex(index)
			err = r.configs.UseConfig(config)
			if err != nil {
				return err
			}
			r.SetCommitIndex(index)
			r.debug("Will bootstrap as leader")
		}
	}

	server, err := r.toFollower(r.GetCurrentTerm())
	r.SetServer(server)
	return err
}

func (r *raft) runRPCServer() error {
	ls, err := net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}
	RegisterRPCServer(r.rpcServer, newRpcServer(r))
	return r.rpcServer.Serve(ls)
}

func (r *raft) Id() RaftId {
	return r.id
}

func (r *raft) Addr() RaftAddr {
	return r.addr
}

func (r *raft) Handle(ctx context.Context, cmd ...Command) error {
	return r.GetServer().Handle(ctx, cmd...)
}

func (r *raft) IsLeader() bool {
	return r.GetServer().IsLeader()
}

func (r *raft) Run() (err error) {
	if atomic.SwapInt32(&r.ran, 1) != 0 {
		return ErrRanRepeatedly
	}

	r.debug("Run raft consensuse module")
	rand.Seed(time.Now().UnixNano())

	go func() {
		err := r.runRPCServer()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			r.debug("run rpc, err: %+v", err)
			os.Exit(1)
		}
	}()

	go r.loopApplyCommitted()

	// drop ticks to avoid election timeout
	for len(r.ticker.C) != 0 {
		<-r.ticker.C
	}
	r.GetServer().ResetTimer()

	for {
		server, err := r.GetServer().Run()
		if errors.Is(err, ErrStopped) {
			return nil
		}
		if err != nil {
			return err
		}
		r.SetServer(server)
	}
}

func (r *raft) Stop() {
	select {
	case _, ok := <-r.done:
		if !ok {
			return
		}
	default:
		// no-op
	}

	close(r.done)
	if r.ticker != nil {
		r.ticker.Stop()
	}
	r.rpcServer.GracefulStop()
	r.rpcClients.Close()
	return
}

// Done 是否已经停止
func (r *raft) Done() <-chan struct{} {
	return r.done
}

func (r *raft) loopApplyCommitted() {
	for {
		select {
		case <-r.done:
			return
		default:
			// no-op
		}

		func() {
			r.commitCond.L.Lock()
			defer r.commitCond.L.Unlock()

			var lastApplied, commitIndex uint64
			for commitIndex <= lastApplied {
				r.commitCond.Wait()
				commitIndex, lastApplied = r.GetCommitIndex(), r.GetLastApplied()
			}

			err := r.applyCommitted()
			if err != nil {
				r.debug("apply commands, err: %+v", err)
			}
		}()
	}
}

// syncLeaderCommit 同步 Leader.CommitIndex
func (r *raft) syncLeaderCommit(leaderCommit uint64) error {
	// 	If leaderCommit > commitIndex,
	//	set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit <= r.GetCommitIndex() {
		return nil
	}
	commitIndex := leaderCommit
	lastIndex, _, err := r.Last()
	if err != nil {
		return err
	}
	if lastIndex < commitIndex {
		commitIndex = lastIndex
	}
	r.state.SetCommitIndex(commitIndex)

	// 通知 commitIndex 更新事件发生
	r.commitCond.Signal()
	return nil
}

// applyCommitted
//
// Implementation:
// 		If commitIndex > lastApplied: increment lastApplied, apply
// 		log[lastApplied] to state machine(§5.3)
func (r *raft) applyCommitted() error {
	commitIndex, lastApplied := r.GetCommitIndex(), r.GetLastApplied()
	if commitIndex <= lastApplied {
		return nil
	}

	// 获取已 commit 且没 apply 的命令
	entries, err := r.RangeGet(lastApplied, commitIndex)
	if err != nil {
		return err
	}

	// apply command type log entries
	var commands = make([]CommandLogEntry, 0, len(entries))
	for i := range entries {
		if entries[i].Type == logEntryTypeCommand {
			entry := entries[i]
			commands = append(commands, CommandLogEntry{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: entry.Command,
			})
		}
	}
	if len(commands) == 0 {
		return nil
	}
	// apply
	appliedCount, err := r.sm.Apply(commands...)
	if err != nil {
		return err
	}

	// update lastApplied
	var count uint64
	for _, entry := range entries {
		if entry.Type == logEntryTypeCommand {
			appliedCount--
		}
		count++
		if appliedCount == 0 {
			break
		}
	}
	r.SetLastApplied(lastApplied + count)
	return nil
}

// sendRPCArgs
// 发送待反应的 rpc Args
func (r *raft) sendRPCArgs(args rpcArgs) {
	if args == nil {
		return
	}
	if args.getTerm() < r.GetCurrentTerm() {
		return
	}
	select {
	case r.rpcArgs <- args:
		// no-op
	default:
		// no-op
	}
}

// reactToRPCArgs
//
// 实现以下功能:
// 		If RPC request or response contains term T > currentTerm:
// 		set currentTerm = T, convert to follower (§5.1)
func (r *raft) reactToRPCArgs(args rpcArgs) (server server, converted bool, err error) {
	if args.getTerm() > r.GetCurrentTerm() {
		r.debug("React to args(term: %d, type: %q)",
			args.getTerm(), args.getType())
		server, err = r.toFollower(args.getTerm())
		if err != nil {
			return nil, false, err
		}
		return server, true, nil
	}
	return nil, false, nil
}

func (r *raft) toFollower(term uint64, votedFor ...RaftId) (server, error) {
	r.SetCurrentTerm(term)
	if len(votedFor) > 0 {
		err := r.SetVotedFor(votedFor[0])
		if err != nil {
			return nil, err
		}
	}
	server := &follower{
		raft: r,
	}
	server.ResetTimer()
	defer r.debug("Convert to follower")

	return server, nil
}

// toCandidate
//
// • On conversion to candidate, start election:
//
// • Increment currentTerm
//
// • Vote for self
//
// • Reset election timer
func (r *raft) toCandidate() server {
	defer r.debug("Convert to candidate")

	nextTerm := r.GetCurrentTerm() + 1
	r.SetCurrentTerm(nextTerm)
	id := r.Id()
	r.SetVotedFor(id)
	server := &candidate{
		raft: r,
	}
	server.ResetTimer()
	return server
}

// toLeader
func (r *raft) toLeader() (server, error) {
	defer r.debug("Convert to leader")

	var mux sync.Mutex
	server := &leader{
		raft:            r,
		ccm:             &mux,
		jointCommitCond: sync.NewCond(&mux),
	}

	// Volatile state on leaders:
	// (Reinitialized after election)
	lastLogIndex, _, err := server.Last()
	if err != nil {
		return nil, err
	}

	peers := r.configs.GetConfig().GetPeers()
	for _, peer := range peers {
		server.nextIndex.Store(peer.Id, lastLogIndex+1)
		server.matchIndex.Store(peer.Id, 0)
	}

	server.ResetTimer()
	return server, nil
}

// heartbeatTimeout 心跳超时
func (r *raft) heartbeatTimeout() time.Duration {
	return r.electionTimeout[0] / 2
}

// randomElectionTimeout 随机选举超时
func (r *raft) randomElectionTimeout() time.Duration {
	start := r.electionTimeout[0]
	end := r.electionTimeout[1]
	d := rand.Int63n(int64(end - start))
	return start + time.Duration(d)
}

// debug
func (r *raft) debug(format string, args ...interface{}) {
	format = fmt.Sprintf("%s %s", r.who(), format)
	r.logger.Debug(format, args...)
}

// who
func (r *raft) who() string {
	// raftId:term:state
	var state string
	if r.GetServer() != nil {
		state = r.GetServer().String()
	}
	for i := 9 - len(state); i > 0; i-- {
		state += " "
	}

	return fmt.Sprintf("[%s:%d:%d:%d:%s]", r.Id(), r.GetCurrentTerm(), r.GetCommitIndex(), r.GetLastApplied(), state)
}

// ChangeConfig add added and remove removed
func (r *raft) ChangeConfig(ctx context.Context, added []RaftPeer, removed []RaftId) error {
	if !r.GetServer().IsLeader() {
		return ErrIsNotLeader
	}

	return r.GetServer().ChangeConfig(ctx, added, removed)
}

// refreshLastHeartbeat
//
// if a server receives a RequestVote
// request within the minimum election timeout
// of hearing from a current leader, it does not update its
// term or grant its vote.
func (r *raft) refreshLastHeartbeat() {
	atomic.StoreInt64(&r.lastHeartbeat, time.Now().UnixMilli())
}

// isLeaderActive
//
// if a server receives a RequestVote
// request within the minimum election timeout
// of hearing from a current leader, it does not update its
// term or grant its vote.
func (r *raft) isLeaderActive() bool {
	lastHeartbeatTime := time.UnixMilli(atomic.LoadInt64(&r.lastHeartbeat))
	return time.Since(lastHeartbeatTime) < r.electionTimeout[0]
}

// loopWaitForLastPersisted
func (r *raft) loopWaitForLastPersisted() error {
	for {
		select {
		case <-r.Done():
			return nil
		default:
			// no-op
		}

		index := r.sm.WaitForLastPersisted()
		err := r.Log.DequeueTo(index)
		if err != nil {
			r.debug("Dequeue to index: %d failed, err: %v", index, err)
		}
	}
}
