package memproxy

import (
	"context"
	"time"
)

// Memcache represents a generic Memcache interface
// implementations of this interface must be thread safe
type Memcache interface {
	Pipeline(ctx context.Context, sess Session, options ...PipelineOption) Pipeline

	// Close ...
	Close() error
}

// Pipeline represents a generic Pipeline
type Pipeline interface {
	LeaseGet(key string, options LeaseGetOptions) func() (LeaseGetResponse, error)
	LeaseSet(key string, data []byte, cas uint64, options LeaseSetOptions) func() (LeaseSetResponse, error)
	Delete(key string, options DeleteOptions) func() (DeleteResponse, error)

	// Execute flush commands to the network
	Execute()

	// Finish must be called after create a Pipeline, often by defer
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

// LeaseSetStatus ...
type LeaseSetStatus uint32

const (
	// LeaseSetStatusStored ...
	LeaseSetStatusStored LeaseSetStatus = iota + 1

	// LeaseSetStatusNotStored NOT stored because of key already been deleted or CAS has changed
	LeaseSetStatusNotStored
)

// LeaseSetResponse lease set response
type LeaseSetResponse struct {
	Status LeaseSetStatus
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
