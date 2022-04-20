package raft

var _ server = (*follower)(nil)

// follower 实现一致性模型在 Follower 下的行为
type follower struct {
	*raft
}

func (f *follower) Run() (server, error) {
	f.ResetTimer()
	for {

		select {
		case <-f.Done():
			return nil, ErrStopped
		case <-f.ticker.C:
			// If election timeout elapses without receiving AppendEntries
			// 	 RPC from current leader or granting vote to candidate:
			// 		convert to candidate
			return f.ToCandidate(), nil
		case term := <-f.rpcTerm:
			server, converted := f.reactToRPCTerm(term)
			if converted {
				return server, nil
			}
		}
	}
}

func (f *follower) Commit(...Command) error {
	return ErrIsNotLeader
}

func (f *follower) ResetTimer() {
	timeout := f.ElectionTimeout()
	f.ticker.Reset(timeout)
}

func (*follower) String() string {
	return "Follower"
}
