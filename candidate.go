package raft

import (
	"context"
	"sync"
)

var _ server = (*candidate)(nil)

// candidate 实现一致性模型在 candidate 状态下的行为
type candidate struct {
	*raft
	once sync.Once
}

func (c *candidate) Run() (server, error) {
	config := c.raft.configs.GetConfig()
	peers := config.GetPeers()
	voteCh, err := c.elect(peers)
	if err != nil {
		return nil, err
	}
	decider := config.NewDecider()

	for {
		select {
		case <-c.Done():
			return nil, ErrStopped
		case args := <-c.rpcArgs:
			server, converted, err := c.reactToRPCArgs(args)
			if err != nil {
				return nil, err
			}
			if converted {
				return server, nil
			}
		case <-c.ticker.C:
			c.debug("Election timeout")
			// If election timeout elapses:
			//	start new election
			return c.toCandidate(), nil
		case voterId, ok := <-voteCh:
			if !ok {
				c.debug("Failed to win the election")
				voteCh = (<-chan RaftId)(nil)
				continue
			}

			//  If votes received from
			//  majority of servers: become leader
			decider.AddVote(voterId)
			if decider.HasAchievedMajority() {
				c.debug("Achieved Majority vote(%v)", decider.Counts())
				return c.toLeader()
			}
		}
	}

}

func (c *candidate) Handle(context.Context, ...Command) error {
	return ErrIsNotLeader
}

func (c *candidate) ResetTimer() {
	c.once.Do(func() {
		c.debug("Reset election timer")
		timeout := c.randomElectionTimeout()
		c.ticker.Reset(timeout)
	})
}

func (c *candidate) String() string {
	return "Candidate"
}

// reactToRPCArgs
//
// • If AppendEntries RPC received from new leader: convert to follower
//
//	While waiting for votes, a candidate may receive an
//	AppendEntries RPC from another server claiming to be
//	leader. If the leader’s term (included in its RPC) is at least
//	as large as the candidate’s current term, then the candidate
//	recognizes the leader as legitimate and returns to follower
//	state. If the term in the RPC is smaller than the candidate’s
//	current term, then the candidate rejects the RPC and continues in candidate state.
func (c *candidate) reactToRPCArgs(args rpcArgs) (server server, converted bool, err error) {
	if args.getType() == rpcArgsTypeAppendEntriesArgs {
		if args.getTerm() >= c.GetCurrentTerm() {
			server, err = c.toFollower(args.getTerm())
			if err != nil {
				return nil, false, err
			}
			return server, true, nil
		} else {
			return nil, false, nil
		}
	}
	return c.raft.reactToRPCArgs(args)
}

// elect
//
// Send RequestVote RPCs to all other servers
func (c *candidate) elect(peers []RaftPeer) (<-chan RaftId, error) {
	lastLogIndex, lastLogTerm, err := c.Last()
	if err != nil {
		return nil, err
	}

	args := RequestVoteArgs{
		Term:         c.GetCurrentTerm(),
		CandidateId:  c.Id(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	voteCh := make(chan RaftId, len(peers))

	go func() {
		defer close(voteCh)
		var wg sync.WaitGroup
		for _, peer := range peers {
			id, addr := peer.Id, peer.Addr
			if c.Id() == id {
				voteCh <- id
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				c.debug("-> Request a vote %s", id)
				results, err := c.rpc.CallRequestVote(addr, args)
				if err != nil {
					c.debug("Call %s's RequestVote, err: %+v", id, err)
					return
				}
				if results.VoteGranted {
					c.debug("<- Vote up %s", id)
					voteCh <- id
				} else {
					c.debug("<- Vote down %s", id)
				}
			}()
		}
		wg.Wait()
	}()

	return voteCh, nil
}

func (*candidate) IsLeader() bool {
	return false
}
