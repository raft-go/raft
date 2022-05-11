package raft

// config  raft config
type config struct {
	// peers peer's id map addr
	peers map[RaftId]RaftAddr
}

// RaftPeer raft peer
type RaftPeer struct {
	Id   RaftId
	Addr RaftAddr
}
