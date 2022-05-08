package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestHandle(t *testing.T) {
	peers := []raftPeer{
		{"1", ":5010"},
		{"2", ":5020"},
		{"3", ":5030"},
		{"4", ":5040"},
		{"5", ":5050"},
		{"6", ":5060"},
		{"7", ":5070"},
	}
	cluster := newCluster(t, peers)
	err := cluster.start()
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	const n = 500 * 100
	commands := make([]Command, 0, n)
	for i := 0; i < n; i++ {
		commands = append(commands, Command(fmt.Sprintf("command %d", i)))
	}
	for i := range commands {
		command := commands[i]
		ctx := context.Background()
		err := cluster.Handle(ctx, command)
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	t.Run("check: apply to state machine", func(t *testing.T) {
		var count int
		for i := range cluster.agents {
			agent := cluster.agents[i]

			if agent.length() > len(commands) {
				t.Errorf("expect <= %d, got  %d", len(commands), agent.length())
			}
			if agent.length() == 0 {
				continue
			}

			for i := 0; i < agent.length(); i++ {
				command := commands[i]
				if got := agent.get(i); bytes.Compare(got, command) != 0 {
					t.Errorf("raft[%s] i: %d, expect apply command: %q, got: %q", agent.raft.Id(), i, command, got)
				}
			}
			if agent.length() == len(commands) {
				count++
			}
		}
		if len(commands) > 0 && count < 1 {
			t.Errorf("expect at least one raft node apply command, but only %d", count)
		}
		t.Logf("apply log entries to %d/%d raft node", count, len(cluster.agents))
	})
}

func newCluster(t *testing.T, peers []raftPeer) *cluster {
	t.Helper()

	var cluster = cluster{
		t: t,
	}
	for i := range peers {
		peer := peers[i]
		agent := &agent{
			t: t,
		}
		raft, err := New(peer.Id, peer.Addr,
			agent.apply, &agent.store, &agent.log,
			cluster.WithOptFns(peer.Id, peer.Addr)...)
		if err != nil {
			t.Fatal(err)
		}
		agent.raft = raft

		cluster.agents = append(cluster.agents, agent)
	}

	return &cluster
}

type cluster struct {
	t      *testing.T
	agents []*agent
	once   sync.Once
}

func (c *cluster) WithOptFns(id RaftId, addr RaftAddr) (optFns []OptFn) {
	c.once.Do(func() {
		optFns = append(optFns, WithBootstrapAsLeader())
	})
	return optFns
}

func (c *cluster) Handle(ctx context.Context, cmd ...Command) error {
	c.t.Helper()

	for i := range c.agents {
		agent := c.agents[i]
		if agent.raft.IsLeader() {
			return agent.raft.Handle(ctx, cmd...)
		}
	}

	return errors.New("no leader")
}

func (c *cluster) getLeader() (Raft, bool) {
	for i := range c.agents {
		if c.agents[i].raft.IsLeader() {
			return c.agents[i].raft, true
		}
	}
	return nil, false
}

func (c *cluster) waitLeaderShip() {
	for {
		time.Sleep(50 * time.Millisecond)
		for i := range c.agents {
			agent := c.agents[i]
			if agent.raft.IsLeader() {
				return
			}
		}
	}
}

func (c *cluster) start() error {
	c.t.Helper()

	for i := range c.agents {
		agent := c.agents[i]
		go func() {
			err := agent.Run()
			if errors.Is(err, ErrStopped) {
				return
			}
			if err != nil {
				c.t.Error(err)
			}
		}()
	}
	c.waitLeaderShip()

	// join cluster
	for i := 0; i < len(c.agents); i++ {
		agent := c.agents[i]
		leader, ok := c.getLeader()
		if !ok {
			panic("leader not exists")
		}
		if leader.Id() == agent.raft.Id() {
			continue
		}

		err := leader.AddServer(agent.raft.Id(), agent.raft.Addr())
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *cluster) Stop() {
	for i := range c.agents {
		c.agents[i].Stop()
	}
}

type agent struct {
	t *testing.T

	store memoryStore
	log   memoryLog

	raft Raft

	mux     sync.Mutex
	applied []Command
}

func (a *agent) length() int {
	a.mux.Lock()
	defer a.mux.Unlock()
	return len(a.applied)
}

func (a *agent) get(index int) Command {
	a.mux.Lock()
	defer a.mux.Unlock()
	return a.applied[index]
}

func (a *agent) append(cmd ...Command) {
	a.mux.Lock()
	defer a.mux.Unlock()
	a.applied = append(a.applied, cmd...)
}

func (a *agent) apply(commands Commands) (appliedCount int, err error) {
	for i := range commands.Data() {
		command := commands.Data()[i]
		a.append(command)
		appliedCount++
	}
	return
}

func (a *agent) Run() error {
	return a.raft.Run()
}

func (a *agent) Stop() {
	a.raft.Stop()
}
