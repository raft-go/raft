# Raft

[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg)](https://godoc.org/github.com/mind1949/raft)
[![Go Report Card](https://goreportcard.com/badge/github.com/mind1949/raft)](https://goreportcard.com/report/github.com/mind1949/raft)

Implement [raft consensus protocol](https://raft.github.io/raft.pdf) .

# Features

- [X] Leader election
- [X] Log replication
- [X] Membership changes (use joint consensus instead of single-server changes)
- [ ] Log compaction

# References

* [The Raft site](https://raftconsensus.github.io/)
* [Raft Paper](https://raft.github.io/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
* [Bugs in single-server membership changes](https://groups.google.com/g/RAFT-dev/c/t4xj6dJTP6E)
* [TiDB 在 Raft 成员变更上踩的坑]()
* [Raft Refloated](https://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf)
