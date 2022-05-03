package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrStopped       = errors.New("err: raft consensus module has been stopped")
	ErrRanRepeatedly = errors.New("err: raft consensus module can not bee ran repeatedly")
)

// New 实例化一个 raft 一致性模型
func New(id RaftId, addr RaftAddr, apply Apply, store Store, log Log, optFns ...OptFn) (Raft, error) {
	opts := newOpts()
	for _, fn := range optFns {
		fn(opts)
	}

	state, err := newState(store)
	if err != nil {
		return nil, err
	}

	raft := &raft{
		id: id,

		state: state,
		Log:   log,

		apply: apply,

		serverAccessor: newServerAccessor(&sync.Mutex{}),

		rpc:  opts.rpc,
		addr: addr,

		commitCond: sync.NewCond(&sync.Mutex{}),
		rpcArgs:    make(chan rpcArgs),

		config:          newConfigStore(store),
		electionTimeout: opts.election,

		logger: opts.logger,

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
	// IsLeader 是否是 Leader
	IsLeader() bool
}

// RaftId raft 一致性模型 id
type RaftId string

func (id RaftId) isNil() bool {
	return id == ""
}

// RaftAddr raft 一致性模型 rpc 通信地址
type RaftAddr string

var _ (Raft) = (*raft)(nil)

// raft 实现 raft 一致性模型
type raft struct {
	id RaftId

	state
	Log

	apply Apply

	serverAccessor

	rpc  RPC
	addr RaftAddr

	// 通知 commitIndex 更新事件发生
	commitCond *sync.Cond

	// 存放 rpc rpcArgs, 方便执行以下操作:
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	rpcArgs chan rpcArgs

	config configStore
	// electionTimeout
	electionTimeout [2]time.Duration

	// ticker heartbeat/election timer
	ticker *time.Ticker

	logger Logger

	// whether or not already ran
	ran int32

	// 表示一致性模型是否已停用
	done chan struct{}
}

func (r *raft) init() (err error) {
	rpc := newRpcWrapper(r, r.rpc)
	r.rpc = rpc

	timeout := r.randomElectionTimeout()
	ticker := time.NewTicker(timeout)
	r.ticker = ticker

	server, err := r.toFollower(r.GetCurrentTerm())
	r.SetServer(server)
	return err
}

func (r *raft) runRPC() error {
	service := r.newRPCService()
	err := r.rpc.Register(service)
	if err != nil {
		return err
	}

	err = r.rpc.Listen(string(r.addr))
	if err != nil {
		return err
	}
	return r.rpc.Serve()
}

func (r *raft) Id() RaftId {
	return r.id
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
		err := r.runRPC()
		if err != nil {
			r.debug("run rpc, err: %+v", err)
			os.Exit(1)
		}
	}()
	defer r.rpc.Close()

	go r.loopApplyCommitted()

	// drop ticks to avoid election timeout
	for len(r.ticker.C) != 0 {
		<-r.ticker.C
	}
	r.GetServer().ResetTimer()

	for {
		server, err := r.GetServer().Run()
		if err != nil {
			return err
		}
		r.SetServer(server)
	}
}

func (r *raft) Stop() {
	close(r.done)
	if r.ticker != nil {
		r.ticker.Stop()
	}
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

// Apply 依序应用 commands 到状态机中
// 返回 应用的 Command 数量 appliedCount
type Apply func(commands Commands) (appliedCount int, err error)

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
	commands := newCommands(entries)

	// apply
	appliedCount, err := r.apply(commands)
	if err != nil {
		return err
	}

	// update lastApplied
	lastApplied += uint64(appliedCount)
	r.SetLastApplied(lastApplied)
	return nil
}

// sendRPCArgs
// 发送待反应的 rpc Args
func (r *raft) sendRPCArgs(args rpcArgs) {
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

func (r *raft) newRPCService() RPCService {
	return &rpcService{
		raft: r,
	}
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

	server := &leader{
		raft: r,
	}

	// Volatile state on leaders:
	// (Reinitialized after election)
	lastLogIndex, _, err := server.Last()
	if err != nil {
		return nil, err
	}
	peers := server.config.Peers()
	for i := range peers {
		raftId := peers[i].Id
		server.nextIndex.Store(raftId, lastLogIndex+1)
		server.matchIndex.Store(raftId, 0)
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
