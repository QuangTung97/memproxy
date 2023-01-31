package item

import (
	"context"
	"errors"
	"github.com/QuangTung97/memproxy"
	"time"
)

// Value ...
type Value interface {
	Marshal() ([]byte, error)
}

// Key ...
type Key interface {
	comparable
	String() string
}

// Unmarshaler ...
type Unmarshaler[T any] func(data []byte) (T, error)

// Filler ...
type Filler[T any, K any] func(ctx context.Context, key K) func() (T, error)

type itemOptions struct {
	sleepDurations []time.Duration
}

// Option ...
type Option func(opts *itemOptions)

// DefaultSleepDurations ...
func DefaultSleepDurations() []time.Duration {
	return []time.Duration{
		2 * time.Millisecond,
		4 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
	}
}

func computeOptions(options []Option) *itemOptions {
	opts := &itemOptions{
		sleepDurations: DefaultSleepDurations(),
	}

	for _, fn := range options {
		fn(opts)
	}
	return opts
}

// WithSleepDurations ...
func WithSleepDurations(durations ...time.Duration) Option {
	return func(opts *itemOptions) {
		opts.sleepDurations = durations
	}
}

// ErrExceededRejectRetryLimit ...
var ErrExceededRejectRetryLimit = errors.New("item: exceeded lease rejected retry limit")

// ErrInvalidLeaseGetStatus ...
var ErrInvalidLeaseGetStatus = errors.New("item: exceeded lease get response status")

// New ...
func New[T Value, K Key](
	pipeline memproxy.Pipeline,
	unmarshaler Unmarshaler[T],
	filler Filler[T, K],
	options ...Option,
) *Item[T, K] {
	return &Item[T, K]{
		options:  computeOptions(options),
		sess:     pipeline.LowerSession(),
		pipeline: pipeline,

		unmarshaler: unmarshaler,
		filler:      filler,
	}
}

// Item ...
type Item[T Value, K Key] struct {
	options     *itemOptions
	sess        memproxy.Session
	pipeline    memproxy.Pipeline
	unmarshaler Unmarshaler[T]
	filler      Filler[T, K]
}

type getResultType[T any] struct {
	resp T
	err  error
}

func (i *Item[T, K]) handleLeaseGranted(
	ctx context.Context, key K, result *getResultType[T],
	keyStr string, cas uint64,
) {
	fillFn := i.filler(ctx, key)
	i.sess.AddNextCall(func() {
		fillResp, err := fillFn()
		if err != nil {
			result.err = err
			return
		}

		result.resp = fillResp
		data, err := fillResp.Marshal()
		if err != nil {
			result.err = err
			return
		}

		i.pipeline.LeaseSet(keyStr, data, cas, memproxy.LeaseSetOptions{})
	})
}

// Get ...
func (i *Item[T, K]) Get(ctx context.Context, key K) func() (T, error) {
	keyStr := key.String()

	// TODO Deduplicate Key

	leaseGetFn := i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})

	var result getResultType[T]
	var nextFn func()
	retryCount := 0

	nextFn = func() {
		leaseGetResp, err := leaseGetFn()
		if err != nil {
			result.err = err
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusFound {
			result.resp, err = i.unmarshaler(leaseGetResp.Data)
			if err != nil {
				result.err = err
			}
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseGranted {
			i.handleLeaseGranted(ctx, key, &result, keyStr, leaseGetResp.CAS)
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseRejected {
			if retryCount >= len(i.options.sleepDurations) {
				result.err = ErrExceededRejectRetryLimit
				return
			}

			i.sess.AddDelayedCall(i.options.sleepDurations[retryCount], func() {
				retryCount++

				leaseGetFn = i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})
				i.sess.AddNextCall(nextFn)
			})
			return
		}

		result.err = ErrInvalidLeaseGetStatus
	}

	i.sess.AddNextCall(nextFn)

	return func() (T, error) {
		i.sess.Execute()
		return result.resp, result.err
	}
}

// LowerSession ...
func (i *Item[T, K]) LowerSession() memproxy.Session {
	return i.sess.GetLower()
}
