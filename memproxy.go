package memproxy

import (
	"context"
	"time"
)

//go:generate moq -rm -out memproxy_mocks.go . Memcache Pipeline SessionProvider Session Filler FillerFactory

// Memcache represents a generic Memcache interface
// implementations of this interface must be thread safe
type Memcache interface {
	Pipeline(ctx context.Context, sess Session, options ...PipelineOption) Pipeline
}

// Pipeline represents a generic Pipeline
type Pipeline interface {
	Get(key string, options GetOptions) func() (GetResponse, error)
	LeaseGet(key string, options LeaseGetOptions) func() (LeaseGetResponse, error)
	LeaseSet(key string, data []byte, cas uint64, options LeaseSetOptions) func() (LeaseSetResponse, error)
	Delete(key string, options DeleteOptions) func() (DeleteResponse, error)
	Execute()
	Finish()
}

// SessionProvider for controlling delayed tasks
type SessionProvider interface {
	New() Session
}

// Session controlling session values & delayed tasks
type Session interface {
	AddNextCall(fn func())
	AddDelayedCall(d time.Duration, fn func())
	Execute()
}

// GetOptions specify GET options
type GetOptions struct {
}

// GetResponse of GET request
type GetResponse struct {
	Found bool
	Data  []byte
}

// LeaseGetOptions lease get options
type LeaseGetOptions struct {
	FillParams any // deprecated TODO
}

// LeaseGetStatus status of lease get
type LeaseGetStatus uint32

const (
	// LeaseGetStatusFound returns Data
	LeaseGetStatusFound LeaseGetStatus = iota + 1

	// LeaseGetStatusLeaseGranted lease granted
	LeaseGetStatusLeaseGranted

	// LeaseGetStatusLeaseRejected lease rejected
	LeaseGetStatusLeaseRejected
)

// LeaseGetResponse lease get response
type LeaseGetResponse struct {
	Status LeaseGetStatus
	CAS    uint64
	Data   []byte
}

// LeaseSetOptions lease set options
type LeaseSetOptions struct {
	TTL uint32
}

// LeaseSetResponse lease set response
type LeaseSetResponse struct {
}

// DeleteOptions delete options
type DeleteOptions struct {
}

// DeleteResponse delete response
type DeleteResponse struct {
}

// FillResponse fill response
type FillResponse struct {
	Data []byte
}

// FillerFactory must be thread safe
type FillerFactory interface {
	New(sess Session, params any) Filler
}

// Filler for filling memcache contents, implementation of this interface NOT need to be thread safe
type Filler interface {
	Fill(ctx context.Context, params any, completeFn func(resp FillResponse, err error))
}

type pipelineOptions struct {
	newFillerParams any
}

func computePipelineOptions(options []PipelineOption) pipelineOptions {
	opts := pipelineOptions{
		newFillerParams: nil,
	}
	for _, fn := range options {
		fn(&opts)
	}
	return opts
}

// PipelineOption ...
type PipelineOption func(opts *pipelineOptions)

// WithNewFillerParams ...
func WithNewFillerParams(params any) PipelineOption {
	return func(opts *pipelineOptions) {
		opts.newFillerParams = params
	}
}
