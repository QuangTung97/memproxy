package memproxy

import (
	"context"
	"time"
)

//go:generate moq -rm -out memproxy_mocks.go . Memcache Pipeline SessionProvider Session

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

	// LowerSession returns a lower priority session
	LowerSession() Session
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

	GetLower() Session
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

type pipelineOptions struct {
}

// PipelineOption ...
type PipelineOption func(opts *pipelineOptions)
