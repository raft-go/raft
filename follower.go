package raft

import "context"

var _ server = (*follower)(nil)

// follower 实现一致性模型在 Follower 下的行为
type follower struct {
	*raft
}

func (f *follower) Run() (server, error) {
	for {
		select {
		case <-f.Done():
			return nil, ErrStopped
		case args := <-f.rpcArgs:
			server, converted, err := f.reactToRPCArgs(args)
			if err != nil {
				return nil, err
			}
			if converted {
				return server, nil
			}
		case <-f.ticker.C:
			f.debug("Election timeout")
			// If election timeout elapses without receiving AppendEntries
			// 	 RPC from current leader or granting vote to candidate:
			// 		convert to candidate
			return f.toCandidate(), nil
		}
	}
}

func (f *follower) Handle(context.Context, ...Command) error {
	return ErrNotLeader
}

func (f *follower) ResetTimer() {
	timeout := f.randomElectionTimeout()
	f.ticker.Reset(timeout)
}

func (*follower) String() string {
	return "Follower"
}

func (*follower) IsLeader() bool {
	return false
}
