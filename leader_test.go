package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRemoveServer(t *testing.T) {
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

	// remove leader from cluster
	leader, ok := cluster.getLeader()
	if !ok {
		t.Fatal("no leader")
	}
	fmt.Println("")
	err = leader.RemoveServer(leader.Id())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if leader.IsLeader() {
		t.Errorf("remove leader failed")
	}

	leader, ok = cluster.getLeader()
	if !ok {
		t.Fatal("no leader")
	}

	var followers []Raft
	for i := range cluster.agents {
		agent := cluster.agents[i]
		if !agent.raft.IsLeader() {
			followers = append(followers, agent.raft)
		}
	}

	var wg sync.WaitGroup
	for i := range followers {
		follower := followers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer follower.Stop()
			for {
				err = leader.RemoveServer(follower.Id())
				if err != nil {
					t.Error(err)
					continue
				}
				return
			}
		}()
	}
	wg.Wait()
}
