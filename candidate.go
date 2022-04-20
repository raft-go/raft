package raft

var _ server = (*candidate)(nil)

// candidate 实现一致性模型在 candidate 状态下的行为
//
//Candidates (§5.2):
// • On conversion to candidate, start election:
// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
// • If votes received from majority of servers: become leader
// • If AppendEntries RPC received from new leader: convert to follower
// • If election timeout elapses: start new election
type candidate struct {
	*raft
}

func (c *candidate) Run() (server, error) {
	c.ResetTimer()
	// TODO:

	return nil, nil
}

func (c *candidate) Commit(...Command) error {
	return ErrIsNotLeader
}

func (c *candidate) ResetTimer() {
	timeout := c.ElectionTimeout()
	c.ticker.Reset(timeout)
}

func (c *candidate) String() string {
	return "Candidate"
}
