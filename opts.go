package raft

import "time"

// OptFn raft 配置可选项
type OptFn func(*opts)

// WithElection 提供选举超时范围
func WithElection(min, max time.Duration) OptFn {
	if min >= max {
		panic("election timeout'min must be less than max")
	}
	return func(o *opts) {
		o.election[0] = min
		o.election[1] = max
	}
}

// WithLogger
func WithLogger(logger Logger) OptFn {
	return func(o *opts) {
		o.logger = logger
	}
}

// WithBootstrapAsLeader bootstrap raft consensus module as leader
func WithBootstrapAsLeader() OptFn {
	return func(o *opts) {
		o.bootstrapAsLeader = true
	}
}

func newOpts() *opts {
	return &opts{
		election: [2]time.Duration{300 * time.Millisecond, 500 * time.Millisecond},
		logger:   newLogger(),
	}
}

// opts raft options
type opts struct {
	// election timeout duration
	election [2]time.Duration
	// bootsTrapAsLeader wether or not bootstrap as leader
	bootstrapAsLeader bool

	logger Logger
}
