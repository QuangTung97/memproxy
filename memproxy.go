package memproxy

import (
	"context"
	"time"
)

// Memcache represents a generic Memcache interface
// implementations of this interface must be thread safe
type Memcache interface {
	// Pipeline creates a Pipeline, a NON thread safe object
	Pipeline(ctx context.Context, options ...PipelineOption) Pipeline

	// Close shutdowns memcache client
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

// SessionProvider for controlling delayed tasks, this object is Thread Safe
type SessionProvider interface {
	New() Session
}

// Session controlling session values & delayed tasks, this object is NOT Thread Safe
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

// ==============================================
// Pipeline Options
// ==============================================

// PipelineConfig ...
type PipelineConfig struct {
	existingSess Session
}

// GetSession ...
func (c *PipelineConfig) GetSession(provider SessionProvider) Session {
	if c.existingSess != nil {
		return c.existingSess
	}
	return provider.New()
}

// ComputePipelineConfig ...
func ComputePipelineConfig(options []PipelineOption) *PipelineConfig {
	conf := &PipelineConfig{
		existingSess: nil,
	}
	for _, fn := range options {
		fn(conf)
	}
	return conf
}

// PipelineOption ...
type PipelineOption func(conf *PipelineConfig)

// WithPipelineExistingSession ...
func WithPipelineExistingSession(sess Session) PipelineOption {
	return func(conf *PipelineConfig) {
		conf.existingSess = sess
	}
}
