package memproxy

import "time"

// Memcache represents a generic Memcache interface
type Memcache interface {
	Pipeline() Pipeline
	PipelineWithSession(sess Session) Pipeline
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

// Session controlling delayed tasks
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
}

// LeaseGetStatus status of lease get
type LeaseGetStatus uint32

const (
	// LeaseGetStatusFound returns data
	LeaseGetStatusFound = iota + 1

	// LeaseGetStatusLeaseGranted lease granted
	LeaseGetStatusLeaseGranted

	// LeaseGetStatusLeaseRejected lease rejected
	LeaseGetStatusLeaseRejected
)

// LeaseGetResponse lease get response
type LeaseGetResponse struct {
	Status LeaseGetStatus
	CAS    uint64
}

// LeaseSetOptions lease set options
type LeaseSetOptions struct {
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
