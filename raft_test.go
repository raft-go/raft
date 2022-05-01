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
	peers := map[RaftId]RaftAddr{
		"1": ":5010",
		"2": ":5020",
		"3": ":5030",
		"4": ":5040",
		"5": ":5050",
		"6": ":5060",
		"7": ":5070",
	}
	cluster := newCluster(t, peers)
	go func() {
		err := cluster.Run()
		if err != nil {
			t.Error(err)
		}
	}()
	cluster.waitLeaderShip()

	const n = 5000
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

	t.Run("check: append log entry", func(t *testing.T) {
		var count int
		for i := range cluster.agents {
			agent := cluster.agents[i]
			entries, err := agent.log.RangeGet(0, uint64(len(commands)))
			if err != nil {
				t.Error(err)
			}

			if len(entries) > len(commands) {
				t.Errorf("expect <= %d, got > %d", len(commands), len(commands))
			}
			if len(entries) == 0 {
				continue
			}

			for i := uint64(0); i < uint64(len(entries)); i++ {
				command := commands[i]
				if got := entries[i].Index; got != i+1 {
					t.Errorf("expect entry index %d, got %d", i+1, got)
				}
				if got := entries[i].Command; bytes.Compare(got, command) != 0 {
					t.Errorf("expect entry command: %q, got: %q", command, got)
				}
			}

			if len(entries) == len(commands) {
				count++
			}
		}
		if len(commands) > 0 && count <= len(cluster.agents)/2 {
			t.Errorf("expect majority raft node append command, but only %d/%d", count, len(cluster.agents))
		}
		t.Logf("append log entries to %d/%d raft node", count, len(cluster.agents))
	})

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

func newCluster(t *testing.T, peers map[RaftId]RaftAddr) *cluster {
	t.Helper()

	var cluster = cluster{
		t: t,
	}
	for id := range peers {
		id := id
		agent := &agent{
			t: t,
		}
		raft, err := New(id, agent.apply, &agent.store, &agent.log, peers)
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

func (c *cluster) Run() error {
	c.t.Helper()

	errCh := make(chan error, 1)
	var once sync.Once
	for i := range c.agents {
		agent := c.agents[i]
		go func() {
			err := agent.Run()
			if err != nil {
				once.Do(func() { errCh <- err })
			}
		}()
	}

	return <-errCh
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
