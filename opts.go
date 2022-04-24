package raft

import "time"

// OptFn raft 配置可选项
type OptFn func(*opts)

// WithRPC 提供 rpc 可选项
func WithRPC(rpc RPC) OptFn {
	return func(o *opts) {
		o.rpc = rpc
	}
}

// WithLogger
func WithLogger(logger Logger) OptFn {
	return func(o *opts) {
		o.logger = logger
	}
}

func defaultOpts() *opts {
	return &opts{
		rpc:      newDefaultRpc(":9797"),
		election: [2]time.Duration{150 * time.Millisecond, 300 * time.Millisecond},
		logger:   newLogger(),
	}
}

// opts raft options
type opts struct {
	// rpc
	rpc RPC
	// election timeout duration
	election [2]time.Duration

	logger Logger
}
