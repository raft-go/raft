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
			f.Debug("Election timeout")
			// If election timeout elapses without receiving AppendEntries
			// 	 RPC from current leader or granting vote to candidate:
			// 		convert to candidate
			return f.ToCandidate(), nil
		}
	}
}

func (f *follower) Handle(context.Context, ...Command) error {
	return ErrIsNotLeader
}

func (f *follower) ResetTimer() {
	timeout := f.ElectionTimeout()
	f.ticker.Reset(timeout)
}

func (*follower) String() string {
	return "Follower"
}
