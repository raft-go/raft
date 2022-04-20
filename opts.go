package raft

import "time"

// OptFn raft 配置可选项
type OptFn func(*opts)

// WithRPCRegister 提供 rpc register 可选项
func WithRPCRegister(register func(RPCService) error) OptFn {
	return func(o *opts) {
		o.register = register
	}
}

// WithRPCClient 提供 rpc client 可选项
func WithRPCClient(client RPCClient) OptFn {
	return func(o *opts) {
		o.client = client
	}
}

func defaultOpts() *opts {
	return &opts{
		register: nil, // TODO:
		client:   nil, // TODO:
		election: [2]time.Duration{150 * time.Microsecond, 300 * time.Microsecond},
	}
}

// opts raft options
type opts struct {
	// register rpc service register
	register func(RPCService) error
	// client rpc client
	client RPCClient
	// election timeout duration
	election [2]time.Duration
}
