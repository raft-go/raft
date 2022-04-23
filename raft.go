package raft

import (
	"context"
	"errors"
	logger "log"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrStopped = errors.New("err: raft consensus module has been stopped")
)

// New 实例化一个 raft 一致性模型
func New(id RaftId, apply Apply, store Store, log Log, peers map[RaftId]RaftAddr, optFns ...OptFn) (Raft, error) {
	if len(peers) == 0 {
		panic("peers can't  be nill")
	}

	opts := defaultOpts()
	for _, fn := range optFns {
		fn(opts)
	}

	state, err := newState(store)
	if err != nil {
		return nil, err
	}
	commitCond := sync.NewCond(&sync.Mutex{})
	raft := &raft{
		id: id,

		state: state,
		Log:   log,

		apply: apply,

		register: opts.register,
		client:   opts.client,

		commitCond: commitCond,
		rpcArgs:    make(chan rpcArgs, 1),

		peers:           peers,
		electionTimeout: opts.election,

		done: make(chan struct{}),
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

	server

	// 注册 rpc 服务
	register func(RPCService) error
	// rpc 服务客户端
	client RPCClient

	// 通知 commitIndex 更新事件发生
	commitCond *sync.Cond

	// 存放 rpc rpcArgs, 方便执行以下操作:
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	rpcArgs chan rpcArgs

	// peers raft 节点
	peers map[RaftId]RaftAddr
	// electionTimeout
	electionTimeout [2]time.Duration

	// ticker heartbeat/election timer
	ticker *time.Ticker

	// 表示一致性模型是否已停用
	done chan struct{}
}

func (r *raft) Id() RaftId {
	return r.id
}

func (r *raft) Run() (err error) {
	rand.Seed(time.Now().UnixNano())

	r.initTicker()

	var server server
	if votedFor := r.GetVotedFor(); r.Id() == votedFor {
		server, err = r.ToLeader()
		if err != nil {
			return err
		}
	} else {
		server = r.ToFollower(votedFor)
	}

	err = r.register(r.newRPCService())
	if err != nil {
		return err
	}

	go r.loopApplyCommitted()

	for {
		server, err = server.Run()
		if err != nil {
			return err
		}
		r.server = server
	}
}

func (r *raft) initTicker() {
	timeout := r.HeartbeatTimout()
	ticker := time.NewTicker(timeout)
	r.ticker = ticker
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

func (r *raft) Handle(ctx context.Context, cmd ...Command) error {
	return r.server.Handle(ctx, cmd...)
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

			var lastApplied, commitIndex int
			for lastApplied <= commitIndex {
				r.commitCond.Wait()
				commitIndex, lastApplied = r.GetCommitIndex(), r.GetLastApplied()
			}

			err := r.ApplyCommitted()
			if err != nil {
				logger.Printf("apply commands, err: %+v", err)
			}
		}()
	}
}

// syncLeaderCommit 同步 Leader.CommitIndex
func (r *raft) syncLeaderCommit(leaderCommit int) error {
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

// ApplyCommitted
//
// Implementation:
// 		If commitIndex > lastApplied: increment lastApplied, apply
// 		log[lastApplied] to state machine(§5.3)
func (r *raft) ApplyCommitted() error {
	var commitIndex, lastApplied int
	if commitIndex <= lastApplied {
		commitIndex, lastApplied = r.GetCommitIndex(), r.GetLastApplied()
	}
	// 获取已 commit 且没 apply 的命令
	entries, err := r.RangeGet(lastApplied, commitIndex)
	if err != nil {
		return err
	}
	var data []Command
	for i := range entries {
		data = append(data, entries[i].Command)
	}
	commands := newCommands(data)

	// apply
	appliedCount, err := r.apply(commands)
	if err != nil {
		return err
	}

	// update lastApplied
	lastApplied += appliedCount
	r.SetLastApplied(lastApplied)
	return nil
}

// reactToRPCArgs
//
// 实现以下功能:
// 		If RPC request or response contains term T > currentTerm:
// 		set currentTerm = T, convert to follower (§5.1)
func (r *raft) reactToRPCArgs(args rpcArgs) (server server, converted bool) {
	if args.getTerm() > r.GetCurrentTerm() {
		return r.ToFollower(args.getCallerId(), args.getTerm()), true
	}
	return nil, false
}

// sendRPCArgs
// 发送待反应的 rpc Args
func (r *raft) sendRPCArgs(args rpcArgs) {
	if args.getTerm() <= r.GetCurrentTerm() {
		return
	}
	select {
	case r.rpcArgs <- args:
		// no-op
	default:
		// no-op
	}
}

func (r *raft) newRPCService() RPCService {
	return &rpcService{
		raft: r,
	}
}

func (r *raft) ToFollower(votedFor RaftId, term ...int) server {
	if len(term) > 0 {
		r.SetCurrentTerm(term[0])
	}
	server := &follower{
		raft: r,
	}
	server.ResetTimer()
	return server
}

// ToCandidate
//
// • On conversion to candidate, start election:
//
// • Increment currentTerm
//
// • Vote for self
//
// • Reset election timer
func (r *raft) ToCandidate() server {
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

// ToLeader
func (r *raft) ToLeader() (server, error) {
	server := &leader{
		raft: r,
	}

	// Volatile state on leaders:
	// (Reinitialized after election)
	lastLogIndex, _, err := server.Last()
	if err != nil {
		return nil, err
	}
	for raftId := range server.peers {
		server.nextIndex.Store(raftId, lastLogIndex+1)
		server.matchIndex.Store(raftId, 0)
	}

	server.ResetTimer()
	return server, nil
}

// HeartbeatTimout 心跳超时
func (r *raft) HeartbeatTimout() time.Duration {
	return r.electionTimeout[0] / 2
}

// ElectionTimeout 随机选举超时
func (r *raft) ElectionTimeout() time.Duration {
	start := r.electionTimeout[0]
	end := r.electionTimeout[1]
	d := rand.Int63n(int64(end - start))
	return start + time.Duration(d)
}
