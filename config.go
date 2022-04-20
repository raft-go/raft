package raft

// config  raft config
type config struct {
	// peers peer's id map addr
	peers map[RaftId]RaftAddr
}
