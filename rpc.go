package raft

import (
	"net"
	"net/http"
	"net/rpc"
)

// RPC raft rpc client and register
type RPC interface {
	Listen() error
	Serve() error
	Register(RPCService) error
	Close() error

	CallAppendEntries(addr RaftAddr, args AppendEntriesArgs) (AppendEntriesResults, error)
	CallRequestVote(addr RaftAddr, args RequestVoteArgs) (RequestVoteResults, error)
}

// RPCService raft rpc service
type RPCService interface {
	AppendEntries(args AppendEntriesArgs, results *AppendEntriesResults) error
	RequestVote(args RequestVoteArgs, results *RequestVoteResults) error
}

type rpcArgsType int8

const (
	_ rpcArgsType = iota
	rpcArgsTypeAppendEntriesArgs
	rpcArgsTypeRequestVoteArgs
)

// rpcArgs
type rpcArgs interface {
	getType() rpcArgsType
	getTerm() int
	getCallerId() RaftId
}

var _ rpcArgs = AppendEntriesArgs{}

// AppendEntries RPC
//
// Invoked by leader to replicate log entries (§5.3);
//	also used as heartbeat (§5.2).
//
// Arguments:
// 	term:
//					leader’s term
// 	leaderId:
//			 		so follower can redirect clients
// 	prevLogIndex:
//					index of log entry immediately preceding new ones
// 	prevLogTerm:
//					term of prevLogIndex entry
// 	entries[]:
//					log entries to store (empty for heartbeat;
// 					may send more than one for efficiency)
// 	leaderCommit:
//					leader’s commitIndex
// Results:
// 	term:
//					currentTerm, for leader to update itself success true
//					if follower contained entry matching
// 	prevLogIndex:
//					and prevLogTerm
//
// Receiver implementation:
//
// 	1. Reply false if term < currentTerm (§5.1)
// 	2. Reply false if log doesn’t contain an entry at prevLogIndex
// 		whose term matches prevLogTerm (§5.3)
// 	3. If an existing entry conflicts with a new one (same index
// 		but different terms), delete the existing entry and all that follow it (§5.3)
// 	4. Append any new entries not already in the log
// 	5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
type AppendEntriesArgs struct {
	Term     int
	LeaderId RaftId

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

func (AppendEntriesArgs) getType() rpcArgsType {
	return rpcArgsTypeAppendEntriesArgs
}

func (a AppendEntriesArgs) getTerm() int {
	return a.Term
}

func (a AppendEntriesArgs) getCallerId() RaftId {
	id := a.LeaderId
	return id
}

type AppendEntriesResults struct {
	Term    int
	Success bool
}

var _ rpcArgs = RequestVoteArgs{}

// RequestVote RPC
//
// Invoked by candidates to gather votes (§5.2).
//
// Arguments:
// 	term candidate’s term
// 	candidateId candidate requesting vote
// 	lastLogIndex index of candidate’s last log entry (§5.4)
// 	lastLogTerm term of candidate’s last log entry (§5.4)
//
// Results:
// 	term:
//				 	currentTerm, for candidate to update itself
// 	voteGranted:
//					true means candidate received vote
//
// Receiver implementation:
// 	1. Reply false if term < currentTerm (§5.1)
// 	2. If votedFor is null or candidateId, and candidate’s log is at
// 		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
type RequestVoteArgs struct {
	Term        int
	CandidateId RaftId

	LastLogIndex int
	LastLogTerm  int
}

func (RequestVoteArgs) getType() rpcArgsType {
	return rpcArgsTypeRequestVoteArgs
}

func (a RequestVoteArgs) getTerm() int {
	return a.Term
}

func (a RequestVoteArgs) getCallerId() RaftId {
	id := a.CandidateId
	return id
}

type RequestVoteResults struct {
	Term        int
	VoteGranted bool
}

var _ RPCService = (*rpcService)(nil)

// rpcService
type rpcService struct {
	*raft
}

// AppendEntries 实现 AppendEntries RPC
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
	s.raft.sendRPCArgs(args)
	s.server.ResetTimer()
	defer func() { results.Term = s.GetCurrentTerm() }()

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
	// 	3. If an existing entry conflicts with a new one (same index
	// 		but different terms), delete the existing entry and all that follow it (§5.3)
	// 	4. Append any new entries not already in the log
	err = s.PopAfter(args.PrevLogIndex)
	if err != nil {
		return err
	}
	err = s.Append(args.Entries...)
	if err != nil {
		return err
	}
	// 	5. If leaderCommit > commitIndex,
	//		set commitIndex = min(leaderCommit, index of last new entry)
	s.syncLeaderCommit(args.LeaderCommit)

	return nil
}

// RequestVote 实现 RequestVote RPC
//
// Receiver implementation:
// 	1. Reply false if term < currentTerm (§5.1)
// 	2. If votedFor is null or candidateId, and candidate’s log is at
// 		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (s *rpcService) RequestVote(args RequestVoteArgs, results *RequestVoteResults) error {
	s.sendRPCArgs(args)
	s.server.ResetTimer()
	defer func() {
		results.Term = s.GetCurrentTerm()
		if results.VoteGranted {
			s.SetVotedFor(args.CandidateId)
		}
	}()

	// 	1. Reply false if term < currentTerm (§5.1)
	currentTerm := s.GetCurrentTerm()
	if args.Term < currentTerm {
		return nil
	}
	// 	2. If votedFor is null or candidateId, and candidate’s log is at
	// 		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if votedFor := s.GetVotedFor(); votedFor.isNil() || args.CandidateId == votedFor {
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
	}

	return nil
}

func newDefaultRpc(addr string) *defaultRPC {
	rpc := &defaultRPC{
		addr: addr,
	}
	return rpc
}

var _ RPC = (*defaultRPC)(nil)

// defaultRPC
type defaultRPC struct {
	addr string

	l net.Listener
}

func (r *defaultRPC) Listen() error {
	var err error
	r.l, err = net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}
	return nil
}
func (r *defaultRPC) Serve() error {
	return http.Serve(r.l, nil)
}

func (r *defaultRPC) Register(service RPCService) error {
	err := rpc.RegisterName("raft", service)
	if err != nil {
		return err
	}
	rpc.HandleHTTP()
	return nil
}

func (r *defaultRPC) Close() error {
	return r.l.Close()
}

func (*defaultRPC) CallAppendEntries(addr RaftAddr, args AppendEntriesArgs) (results AppendEntriesResults, err error) {
	client, err := rpc.DialHTTP("tcp", string(addr))
	if err != nil {
		return results, err
	}
	err = client.Call("raft.AppendEntries", args, &results)
	return results, err
}

func (*defaultRPC) CallRequestVote(addr RaftAddr, args RequestVoteArgs) (results RequestVoteResults, err error) {
	client, err := rpc.DialHTTP("tcp", string(addr))
	if err != nil {
		return results, err
	}
	err = client.Call("raft.RequestVote", args, &results)
	return results, err
}
