package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrStopped       = errors.New("err: raft consensus module has been stopped")
	ErrCommitTimeout = errors.New("err: commit comands timeout")
)

// New 实例化一个 raft 一致性模型
func New(id RaftId, store Store, log Log, peers map[RaftId]RaftAddr, optFns ...OptFn) (Raft, error) {
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

	//  在 timeout 内完成提交命令cmd
	Commit(timeout time.Duration, cmd ...Command) error
	// 阻塞式获取服务未处理的的命令
	ApplyCommands(apply Apply) error
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

func (r *raft) Run() error {
	rand.Seed(time.Now().UnixNano())

	r.initTicker()

	var server server
	if votedFor := r.GetVotedFor(); r.Id() == votedFor {
		server = r.ToLeader()
	} else {
		server = r.ToFollower(votedFor)
	}

	err := r.register(r.newRPCService())
	if err != nil {
		return err
	}

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

func (r *raft) Commit(timeout time.Duration, cmd ...Command) error {
	return r.server.Commit(timeout, cmd...)
}

// syncLeaderCommit 同步 Leader.CommitIndex
func (r *raft) syncLeaderCommit(leaderCommit int) {
	// 	If leaderCommit > commitIndex,
	//	set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit <= r.GetCommitIndex() {
		return
	}
	commitIndex := leaderCommit
	lastIndex, _ := r.Last()
	if lastIndex < commitIndex {
		commitIndex = lastIndex
	}
	r.state.SetCommitIndex(commitIndex)

	// if commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)
	r.commitCond.Signal()
}

// Apply 依序应用 commands 到状态机中
// 返回 应用的 Command 数量 appliedCount
type Apply func(commands Commands) (appliedCount int, err error)

// ApplyCommands
//
// Implementation:
// 		If commitIndex > lastApplied: increment lastApplied, apply
// 		log[lastApplied] to state machine(§5.3)
func (r *raft) ApplyCommands(apply Apply) (err error) {
	r.commitCond.L.Lock()
	defer r.commitCond.L.Unlock()

	// 等待, 直到 commitIndex 更新
	var commitIndex, lastApplied int
	for commitIndex <= lastApplied {
		r.commitCond.Wait()
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
	appliedCount, err := apply(commands)
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
	if args.GetTerm() > r.GetCurrentTerm() {
		return r.ToFollower(args.GetCallerId(), args.GetTerm()), true
	}
	return nil, false
}

// sendRPCArgs
// 发送待反应的 rpc Args
func (r *raft) sendRPCArgs(args rpcArgs) {
	if args.GetTerm() <= r.GetCurrentTerm() {
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
func (r *raft) ToLeader() server {
	server := &leader{
		raft: r,
	}

	// Volatile state on leaders:
	// (Reinitialized after election)
	lastLogIndex, _ := server.Last()
	for raftId := range server.peers {
		server.nextIndex.Store(raftId, lastLogIndex+1)
		server.matchIndex.Store(raftId, 0)
	}

	server.ResetTimer()
	return server
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
