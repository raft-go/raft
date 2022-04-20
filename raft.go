package raft

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrStopped       = errors.New("err: raft consensus module has been stopped")
	ErrCommitTimeout = errors.New("err: commit comands timeout")
)

// New 实例化一个 raft 一致性模型
func New(id RaftId, peers map[RaftId]RaftAddr, optFns ...OptFn) Raft {
	if len(peers) == 0 {
		panic("peers can't  be nill")
	}

	opts := defaultOpts()
	for _, fn := range optFns {
		fn(opts)
	}

	applyCond := sync.NewCond(&sync.Mutex{})
	raft := &raft{
		id: id,

		state: nil, // TODO:

		register: opts.register,
		client:   opts.client,

		applyCond: applyCond,
		rpcArgs:   make(chan rpcArgs, 1),

		peers:           peers,
		electionTimeout: opts.election,

		done: make(chan struct{}),
	}

	return raft
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
	GetCommands(ackTimeout time.Duration) (Commands, error)
}

// RaftId raft 一致性模型 id
type RaftId string

// Equal 比较两个 RaftId 是否相同
func (id RaftId) Equal(i *RaftId) bool {
	if i == nil {
		return false
	}
	return id == *i
}

// RaftAddr raft 一致性模型 rpc 通信地址
type RaftAddr string

var _ (Raft) = (*raft)(nil)

// raft 实现 raft 一致性模型
type raft struct {
	id RaftId

	state

	server

	// 注册 rpc 服务
	register func(RPCService) error
	// rpc 服务客户端
	client RPCClient

	// 0表示无命令等待 Ack, 否则表示有
	waitAck int32
	// 当 commitIndex 更新时, 通过 applyCond 通知处理命令
	applyCond *sync.Cond

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

	server, err := r.Load()
	if err != nil {
		return err
	}

	err = r.register(r.newRPCService())
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
	lastIndex := r.Len()
	if lastIndex < commitIndex {
		commitIndex = lastIndex
	}
	r.state.SetCommitIndex(commitIndex)

	// if commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)
	r.applyCond.Signal()
}

// GetCommands
//
// Implementation:
// 		If commitIndex > lastApplied: increment lastApplied, apply
// 		log[lastApplied] to state machine(§5.3)
//
// 实现逻辑:
// 		1. 若有命令等待 Ack , 则不等待, 否则等待 r.applyCond.Wait()
//		2. 获取已经提交且没有 apply 的命令, 生成返回 commands
//		3. 若在 ackTimeout 内调用了 commands.Ack 方法,
//			则会更新 lastApplied 为 commitIndex
//			否则无任何作用
func (r *raft) GetCommands(ackTimeout time.Duration) (commands Commands, err error) {
	// FIXME: ackTimeout 要有什么限制?

	// 若没有等待 Ack 的命令, 则等待信号
	if atomic.LoadInt32(&r.waitAck) == 0 {
		r.applyCond.L.Unlock()
		defer r.applyCond.L.Lock()
		r.applyCond.Wait()
	}

	// 获取已经提交, 但没有 apply 的命令
	commitIndex, lastApplied := r.GetCommitIndex(), r.GetLastApplied()
	if commitIndex <= lastApplied {
		return nil, err
	}
	entries, err := r.RangeGet(lastApplied, commitIndex)
	if err != nil {
		return nil, err
	}
	var data []Command
	for i := range entries {
		data = append(data, entries[i].Command)
	}

	timer := time.NewTimer(ackTimeout)
	ack := func() error {
		// 若未超时
		if timer.Stop() {
			atomic.StoreInt32(&r.waitAck, 0) // 标记无命令等待 Ack
			r.SetLastApplied(commitIndex)    // 更新 lastApplied
		}
		return nil
	}
	commands = newCommands(data, ack)
	atomic.StoreInt32(&r.waitAck, 1) // 标记有命令等待 Ack

	return commands, nil
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

func (r *raft) Load() (server, error) {
	err := r.state.Load()
	if err != nil {
		return nil, err
	}

	if votedFor := r.GetVotedFor(); r.Id().Equal(votedFor) {
		return r.ToLeader(), nil
	}
	return r.ToFollower(r.GetVotedFor()), nil
}

func (r *raft) newRPCService() RPCService {
	return &rpcService{
		raft: r,
	}
}

func (r *raft) ToFollower(votedFor *RaftId, term ...int) server {
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
	r.SetVotedFor(&id)
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
