package raft

import (
	"context"
	"sync"
)

var _ server = (*candidate)(nil)

// candidate 实现一致性模型在 candidate 状态下的行为
type candidate struct {
	*raft
}

func (c *candidate) Run() (server, error) {
	var count int
	voteCh, err := c.elect()
	if err != nil {
		return nil, err
	}
	for range voteCh {
		count++
		// • If votes received from
		//   majority of servers: become leader
		if count > len(c.peers)/2 {
			return c.ToLeader()
		}
	}

	for {
		select {
		case <-c.Done():
			return nil, ErrStopped
		case <-c.ticker.C:
			// If election timeout elapses:
			//	start new election
			return c.ToCandidate(), nil
		case args := <-c.rpcArgs:
			server, converted := c.reactToRPCArgs(args)
			if converted {
				return server, nil
			}
		}
	}
}

func (c *candidate) Handle(context.Context, ...Command) error {
	return ErrIsNotLeader
}

func (c *candidate) ResetTimer() {
	timeout := c.ElectionTimeout()
	c.ticker.Reset(timeout)
}

func (c *candidate) String() string {
	return "Candidate"
}

// reactToRPCArgs
//
// • If AppendEntries RPC received from new leader: convert to follower
//
//	While waiting for votes, a candidate may receive an
// 	AppendEntries RPC from another server claiming to be
// 	leader. If the leader’s term (included in its RPC) is at least
// 	as large as the candidate’s current term, then the candidate
// 	recognizes the leader as legitimate and returns to follower
// 	state. If the term in the RPC is smaller than the candidate’s
// 	current term, then the candidate rejects the RPC and continues in candidate state.
func (c *candidate) reactToRPCArgs(args rpcArgs) (server server, converted bool) {
	if args.getType() == rpcArgsTypeAppendEntriesArgs {
		if args.getTerm() >= c.GetCurrentTerm() {
			return c.ToFollower(args.getCallerId()), true
		} else {
			return nil, false
		}
	}
	return c.raft.reactToRPCArgs(args)
}

// elect
//
// Send RequestVote RPCs to all other servers
func (c *candidate) elect() (<-chan struct{}, error) {
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

	voteCh := make(chan struct{}, len(c.peers))

	go func() {
		defer close(voteCh)
		var wg sync.WaitGroup
		for id, addr := range c.peers {
			id, addr := id, addr
			if c.Id() == id {
				voteCh <- struct{}{}
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				results, err := c.rpc.CallRequestVote(addr, args)
				if err != nil {
					return
				}
				if results.VoteGranted {
					voteCh <- struct{}{}
				}
			}()
		}
		wg.Wait()
	}()

	return voteCh, nil
}
