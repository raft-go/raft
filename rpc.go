package raft

import (
	"context"
	"errors"
	"sync"

	grpc "google.golang.org/grpc"
)

type rpcArgsType int8

const (
	_ rpcArgsType = iota
	rpcArgsTypeAppendEntriesArgs
	rpcArgsTypeAppendEntriesResults

	rpcArgsTypeRequestVoteArgs
	rpcArgsTypeRequestVoteResults

	rpcArgsTypeInstallSnapshotArgs
	rpcArgsTypeInstallSnapshotResults
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
	case rpcArgsTypeInstallSnapshotArgs:
		return "InstallSnapshotArgs"
	case rpcArgsTypeInstallSnapshotResults:
		return "InstallSnapshotResults"
	default:
		return "Unknown rpcArgsType"
	}
}

// rpcArgs
type rpcArgs interface {
	getType() rpcArgsType
	getTerm() uint64
}

var _ rpcArgs = (*AppendEntriesArgs)(nil)

func (*AppendEntriesArgs) getType() rpcArgsType {
	return rpcArgsTypeAppendEntriesArgs
}

func (a *AppendEntriesArgs) getTerm() uint64 {
	return a.Term
}

var _ rpcArgs = (*AppendEntriesResults)(nil)

func (*AppendEntriesResults) getType() rpcArgsType {
	return rpcArgsTypeAppendEntriesResults
}

func (a *AppendEntriesResults) getTerm() uint64 {
	return a.Term
}

var _ rpcArgs = (*RequestVoteArgs)(nil)

func (*RequestVoteArgs) getType() rpcArgsType {
	return rpcArgsTypeRequestVoteArgs
}

func (a *RequestVoteArgs) getTerm() uint64 {
	return a.Term
}

var _ rpcArgs = (*RequestVoteResults)(nil)

func (*RequestVoteResults) getType() rpcArgsType {
	return rpcArgsTypeRequestVoteResults
}

func (r *RequestVoteResults) getTerm() uint64 {
	return r.Term
}

var _ RPCServer = (*rpcServer)(nil)

func newRpcServer(r *raft) *rpcServer {
	return &rpcServer{
		raft: r,
	}
}

// rpcServer
type rpcServer struct {
	mu sync.Mutex
	*raft

	UnimplementedRPCServer
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
func (s *rpcServer) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (results *AppendEntriesResults, err error) {
	s.refreshLastHeartbeat()
	s.raft.sendRPCArgs(args)
	s.GetServer().ResetTimer()

	results = &AppendEntriesResults{}
	defer func() {
		results.Term = s.GetCurrentTerm()
	}()

	currentTerm := s.GetCurrentTerm()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		return results, nil
	}
	// 	2. Reply false if log doesn’t contain an entry at prevLogIndex
	// 		whose term matches prevLogTerm (§5.3)
	match, err := s.Match(args.PrevLogIndex, args.PrevLogTerm)
	if err != nil {
		return results, err
	}
	if !match {
		return results, nil
	}
	results.Success = true
	// 	3. If an existing entry conflicts with a new one (same index
	// 		but different terms), delete the existing entry and all that follow it (§5.3)
	// 	4. Append any new entries not already in the log
	if len(args.Entries) > 0 {
		entries := make([]LogEntry, 0, len(args.Entries))
		for i := range args.Entries {
			entry := args.Entries[i]
			entries = append(entries, LogEntry{
				Index:      entry.Index,
				Term:       entry.Term,
				Type:       entry.Type,
				Command:    entry.Command,
				AppendTime: entry.AppendTime.AsTime(),
			})
		}
		err = s.raft.Log.AppendAfter(args.PrevLogIndex, entries...)
		if err != nil {
			return results, err
		}

		// fallback config if config log entry is delete
		config := s.raft.configs.GetConfig()
		for config.GetIndex() > args.PrevLogIndex {
			err = s.raft.configs.FallbackConfig()
			if err != nil {
				return results, err
			}
			config = s.raft.configs.GetConfig()
		}
		// Once a given server adds the new configuration entry to its log,
		// it uses that configuration for all future decisions
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + uint64(i) + 1
			if entry.Type == logEntryTypeConfig {
				config, err := s.raft.configs.NewConfig(index, entry.Command)
				if err != nil {
					return results, err
				}
				s.raft.configs.UseConfig(config)

				if config.IsJoint() {
					s.raft.debug("~> C(old,new): %v", config)
				} else {
					s.raft.debug("~> C(new): %v", config)
				}
			}
		}
	}
	// 	5. If leaderCommit > commitIndex,
	//		set commitIndex = min(leaderCommit, index of last new entry)
	s.syncLeaderCommit(args.LeaderCommit)

	return results, nil
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
func (s *rpcServer) RequestVote(ctx context.Context, args *RequestVoteArgs) (results *RequestVoteResults, err error) {
	results = &RequestVoteResults{}
	if s.isLeaderActive() {
		return results, nil
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
			s.SetVotedFor(RaftId(args.CandidateId))
		} else {
			s.debug("-> Vote down %s at %d", args.CandidateId, args.Term)
		}
	}()

	// 	1. Reply false if term < currentTerm (§5.1)
	currentTerm := s.GetCurrentTerm()
	if args.Term < currentTerm {
		return results, nil
	}
	// 	2. If votedFor is null or candidateId, and candidate’s log is at
	// 		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	votedFor := s.GetVotedFor()
	if currentTerm == args.Term {
		if !(votedFor == "" || RaftId(args.CandidateId) == votedFor) {
			return results, nil
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
		return results, err
	}
	if term < args.LastLogTerm {
		results.VoteGranted = true
		return results, nil
	}
	if term == args.LastLogTerm && index <= args.LastLogIndex {
		results.VoteGranted = true
		return results, nil
	}

	return results, nil
}

// InstallSnapshot
//
// Invoked by leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order.
//
// Receiver implementation:
// 4. Reply and wait for more data chunks if done is false
// 5. Save snapshot file, discard any existing or partial snapshot
// with a smaller index
// 6. If existing log entry has same index and term as snapshot’s
// last included entry, retain log entries following it and reply
// 7. Discard the entire log
// 8. Reset state machine using snapshot contents (and load
// snapshot’s cluster configuration)
func (s *rpcServer) InstallSnapshot(args RPC_InstallSnapshotServer) error {
	// TODO:
	return errors.New("not implement")
}

func newRpcClients(raft *raft) *rpcClients {
	return &rpcClients{
		raft:    raft,
		clients: make(map[string]RPCClient),
	}
}

// rpcClients
type rpcClients struct {
	*raft
	mux     sync.RWMutex
	clients map[string]RPCClient

	conns []*grpc.ClientConn
}

func (c *rpcClients) getClient(ctx context.Context, addr RaftAddr) (RPCClient, error) {
	c.mux.RLock()
	client, ok := c.clients[addr]
	c.mux.RUnlock()
	if ok {
		return client, nil
	}

	c.mux.Lock()
	defer c.mux.Unlock()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.conns = append(c.conns, conn)
	client = NewRPCClient(conn)
	c.clients[addr] = client
	return client, nil
}

func (c *rpcClients) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	for _, conn := range c.conns {
		conn.Close()
	}
	return nil
}

func (c *rpcClients) CallAppendEntries(ctx context.Context, addr RaftAddr, args *AppendEntriesArgs, opts ...grpc.CallOption) (results *AppendEntriesResults, err error) {
	client, err := c.getClient(ctx, addr)
	if err != nil {
		return nil, err
	}

	results, err = client.AppendEntries(ctx, args)
	c.raft.sendRPCArgs(results)
	return results, err
}

func (c *rpcClients) CallRequestVote(ctx context.Context, addr RaftAddr, args *RequestVoteArgs, opts ...grpc.CallOption) (results *RequestVoteResults, err error) {
	client, err := c.getClient(ctx, addr)
	if err != nil {
		return nil, err
	}

	results, err = client.RequestVote(ctx, args)
	c.raft.sendRPCArgs(results)
	return results, err
}

func (c *rpcClients) CallInstallSnapshot(ctx context.Context, addr RaftAddr, opts ...grpc.CallOption) (results RPC_InstallSnapshotClient, err error) {
	client, err := c.getClient(ctx, addr)
	if err != nil {
		return nil, err
	}

	return client.InstallSnapshot(ctx, opts...)
}
