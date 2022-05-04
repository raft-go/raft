package raft

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// RPC raft rpc client and register
type RPC interface {
	Listen(addr string) error
	Serve() error
	Register(RPCService) error
	Close() error

	CallAppendEntries(addr RaftAddr, args AppendEntriesArgs) (AppendEntriesResults, error)
	CallRequestVote(addr RaftAddr, args RequestVoteArgs) (RequestVoteResults, error)
}

// RPCService raft rpc service
type RPCService interface {
	// Invoked by leader to replicate log entries (§5.3);
	//	also used as heartbeat (§5.2).
	AppendEntries(args AppendEntriesArgs, results *AppendEntriesResults) error
	// Invoked by candidates to gather votes (§5.2).
	RequestVote(args RequestVoteArgs, results *RequestVoteResults) error
}

type rpcArgsType int8

const (
	_ rpcArgsType = iota
	rpcArgsTypeAppendEntriesArgs
	rpcArgsTypeAppendEntriesResults
	rpcArgsTypeRequestVoteArgs
	rpcArgsTypeRequestVoteResults
)

func (t rpcArgsType) String() string {
	switch t {
	case rpcArgsTypeAppendEntriesArgs:
		return "AppendEntriesArgs"
	case rpcArgsTypeAppendEntriesResults:
		return "AppendEntriesResults"
	case rpcArgsTypeRequestVoteArgs:
		return "RequestVoteArgs"
	case rpcArgsTypeRequestVoteResults:
		return "RequestVoteResults"
	default:
		return "Unknown rpcArgsType"
	}
}

// rpcArgs
type rpcArgs interface {
	getType() rpcArgsType
	getTerm() uint64
}

var _ rpcArgs = AppendEntriesArgs{}

// AppendEntriesArgs
type AppendEntriesArgs struct {
	// leader’s term
	Term uint64
	// so follower can redirect clients
	LeaderId RaftId

	// index of log entry immediately preceding new ones
	PrevLogIndex uint64
	// term of prevLogIndex entry
	PrevLogTerm uint64

	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries []LogEntry

	// leader’s commitIndex
	LeaderCommit uint64
}

func (AppendEntriesArgs) getType() rpcArgsType {
	return rpcArgsTypeAppendEntriesArgs
}

func (a AppendEntriesArgs) getTerm() uint64 {
	return a.Term
}

var _ rpcArgs = AppendEntriesResults{}

// AppendEntriesResults
type AppendEntriesResults struct {
	// currentTerm
	Term uint64
	// for leader to update itself success true
	// if follower contained entry matching
	Success bool
}

func (AppendEntriesResults) getType() rpcArgsType {
	return rpcArgsTypeAppendEntriesResults
}

func (a AppendEntriesResults) getTerm() uint64 {
	return a.Term
}

var _ rpcArgs = RequestVoteArgs{}

// RequestVoteArgs
type RequestVoteArgs struct {
	// term candidate’s term
	Term uint64
	// candidateId candidate requesting vote
	CandidateId RaftId

	// lastLogIndex index of candidate’s last log entry (§5.4)
	LastLogIndex uint64
	// lastLogTerm term of candidate’s last log entry (§5.4)
	LastLogTerm uint64
}

func (RequestVoteArgs) getType() rpcArgsType {
	return rpcArgsTypeRequestVoteArgs
}

func (a RequestVoteArgs) getTerm() uint64 {
	return a.Term
}

var _ rpcArgs = RequestVoteResults{}

// RequestVoteResults
type RequestVoteResults struct {
	// currentTerm, for candidate to update itself
	Term uint64
	// true means candidate received vote
	VoteGranted bool
}

func (RequestVoteResults) getType() rpcArgsType {
	return rpcArgsTypeRequestVoteResults
}

func (r RequestVoteResults) getTerm() uint64 {
	return r.Term
}

var _ RPCService = (*rpcService)(nil)

// rpcService
type rpcService struct {
	mu sync.Mutex
	*raft
}

// AppendEntries 实现 AppendEntries RPC
//
// Invoked by leader to replicate log entries (§5.3);
//	also used as heartbeat (§5.2).
//
// Implementation:
//
// 	1. Reply false if term < currentTerm (§5.1)
// 	2. Reply false if log doesn’t contain an entry at prevLogIndex
// 		whose term matches prevLogTerm (§5.3)
// 	3. If an existing entry conflicts with a new one (same index
// 		but different terms), delete the existing entry and all that follow it (§5.3)
// 	4. Append any new entries not already in the log
// 	5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *rpcService) AppendEntries(args AppendEntriesArgs, results *AppendEntriesResults) error {
	s.refreshLastHeartbeat()
	s.raft.sendRPCArgs(args)
	s.GetServer().ResetTimer()
	defer func() {
		results.Term = s.GetCurrentTerm()
	}()

	currentTerm := s.GetCurrentTerm()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		return nil
	}
	// 	2. Reply false if log doesn’t contain an entry at prevLogIndex
	// 		whose term matches prevLogTerm (§5.3)
	match, err := s.Match(args.PrevLogIndex, args.PrevLogTerm)
	if err != nil {
		return err
	}
	if !match {
		return nil
	}
	results.Success = true
	// 	3. If an existing entry conflicts with a new one (same index
	// 		but different terms), delete the existing entry and all that follow it (§5.3)
	// 	4. Append any new entries not already in the log
	if len(args.Entries) > 0 {
		err = s.PopAfter(args.PrevLogIndex)
		if err != nil {
			return err
		}
		// Instead, each server adopts Cnew as soon as that entry exists in its
		// log, and the leader knows it’s safe to allow further configuration
		// changes as soon as the Cnew entry has been committed.
		// Unfortunately, this decision does imply that a log entry for a configuration
		// change can be removed (if leadership changes);
		// in this case, a server must be prepared to fall back
		// to the previous configuration in its log.
		if s.config.LogIndex() > args.PrevLogIndex {
			s.config.FallBack()
		}

		err = s.Append(args.Entries...)
		if err != nil {
			return err
		}
	}
	// 	5. If leaderCommit > commitIndex,
	//		set commitIndex = min(leaderCommit, index of last new entry)
	s.syncLeaderCommit(args.LeaderCommit)

	return nil
}

// RequestVote 实现 RequestVote RPC
//
// Invoked by candidates to gather votes (§5.2).
//
// implementation:
//
// 	1. Reply false if term < currentTerm (§5.1)
// 	2. Reply false if log doesn’t contain an entry at prevLogIndex
// 		whose term matches prevLogTerm (§5.3)
// 	3. If an existing entry conflicts with a new one (same index
// 		but different terms), delete the existing entry and all that follow it (§5.3)
// 	4. Append any new entries not already in the log
// 	5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *rpcService) RequestVote(args RequestVoteArgs, results *RequestVoteResults) error {
	if s.isLeaderActive() {
		return nil
	}
	// 加锁, 防止两个 term 相同
	// 且比 currentTerm 大的节点同时获得投票
	s.mu.Lock()
	defer s.mu.Unlock()

	s.debug("<- Vote request %s at %d", args.CandidateId, args.Term)
	s.sendRPCArgs(args)
	s.GetServer().ResetTimer()
	defer func() {
		results.Term = s.GetCurrentTerm()
		if results.VoteGranted {
			s.debug("-> Vote up %s at %d", args.CandidateId, args.Term)
			s.SetVotedFor(args.CandidateId)
		} else {
			s.debug("-> Vote down %s at %d", args.CandidateId, args.Term)
		}
	}()

	// 	1. Reply false if term < currentTerm (§5.1)
	currentTerm := s.GetCurrentTerm()
	if args.Term < currentTerm {
		return nil
	}
	// 	2. If votedFor is null or candidateId, and candidate’s log is at
	// 		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	votedFor := s.GetVotedFor()
	if currentTerm == args.Term {
		if !(votedFor.isNil() || args.CandidateId == votedFor) {
			return nil
		}
	}

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs.
	//
	// If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date.
	//
	// If the logs end with the same term, then whichever log is longer is
	// more up-to-date.
	index, term, err := s.Last()
	if err != nil {
		return err
	}
	if term < args.LastLogTerm {
		results.VoteGranted = true
		return nil
	}
	if term == args.LastLogTerm && index <= args.LastLogIndex {
		results.VoteGranted = true
		return nil
	}

	return nil
}

func newDefaultRpc() *defaultRPC {
	rpc := &defaultRPC{
		server: rpc.NewServer(),
	}
	return rpc
}

var _ RPC = (*defaultRPC)(nil)

// defaultRPC
type defaultRPC struct {
	id RaftId

	l      net.Listener
	server *rpc.Server

	clients rpcClients
}

func (r *defaultRPC) Listen(addr string) error {
	var err error
	r.l, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return nil
}

func (r *defaultRPC) Serve() error {
	return http.Serve(r.l, r.server)
}

func (r *defaultRPC) Register(service RPCService) error {
	return r.server.RegisterName("raft", service)
}

func (r *defaultRPC) Close() error {
	closes := []func() error{
		r.l.Close,
		r.clients.Close,
	}
	for _, close := range closes {
		_ = close()
	}
	return nil
}

func (r *defaultRPC) CallAppendEntries(addr RaftAddr, args AppendEntriesArgs) (results AppendEntriesResults, err error) {
	client, err := r.clients.Get(addr)
	if err != nil {
		return results, err
	}

	err = client.Call("raft.AppendEntries", args, &results)
	if errors.Is(err, net.ErrClosed) {
		r.clients.Delete(addr)
	}
	return results, err
}

func (r *defaultRPC) CallRequestVote(addr RaftAddr, args RequestVoteArgs) (results RequestVoteResults, err error) {
	client, err := r.clients.Get(addr)
	if err != nil {
		return results, err
	}

	err = client.Call("raft.RequestVote", args, &results)
	if errors.Is(err, net.ErrClosed) {
		r.clients.Delete(addr)
	}
	return results, err
}

// rpcClients reuse rpc.Client
type rpcClients struct {
	mux     sync.RWMutex
	clients map[RaftAddr]*rpc.Client
	closed  bool
}

func (c *rpcClients) Get(addr RaftAddr) (*rpc.Client, error) {
	c.mux.RLock()
	if c.clients != nil {
		client, ok := c.clients[addr]
		if ok {
			c.mux.RUnlock()
			return client, nil
		}
	}
	c.mux.RUnlock()

	c.mux.Lock()
	defer c.mux.Unlock()
	if c.clients == nil {
		c.clients = make(map[RaftAddr]*rpc.Client)
	}
	client, err := rpc.DialHTTP("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	c.clients[addr] = client
	return client, nil
}

func (c *rpcClients) Delete(addr RaftAddr) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.clients == nil {
		c.clients = make(map[RaftAddr]*rpc.Client)
	}
	delete(c.clients, addr)
}

func (c *rpcClients) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.closed {
		return nil
	}

	for _, client := range c.clients {
		_ = client.Close()
	}
	c.clients = nil
	defer func() { c.closed = true }()
	return nil
}

var _ RPC = (*rpcWrapper)(nil)

func newRpcWrapper(raft *raft, rpc RPC) *rpcWrapper {
	return &rpcWrapper{
		raft: raft,
		RPC:  rpc,
	}
}

// rpcWrapper
type rpcWrapper struct {
	*raft
	RPC
}

func (w *rpcWrapper) CallAppendEntries(addr RaftAddr, args AppendEntriesArgs) (results AppendEntriesResults, err error) {
	results, err = w.RPC.CallAppendEntries(addr, args)
	w.raft.sendRPCArgs(results)
	return results, err
}

func (w *rpcWrapper) CallRequestVote(addr RaftAddr, args RequestVoteArgs) (results RequestVoteResults, err error) {
	results, err = w.RPC.CallRequestVote(addr, args)
	w.raft.sendRPCArgs(results)
	return results, err
}
