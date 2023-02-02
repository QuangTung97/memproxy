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

// ErrExceededRejectRetryLimit TODO Remove this error
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

		getKeys: map[K]getResultType[T]{},
	}
}

// Item is NOT thread safe and, it contains a cached keys
// once a key is cached in memory, it will return the same value unless call **Reset**
type Item[T Value, K Key] struct {
	options     *itemOptions
	sess        memproxy.Session
	pipeline    memproxy.Pipeline
	unmarshaler Unmarshaler[T]
	filler      Filler[T, K]

	getKeys map[K]getResultType[T]
}

type getResultType[T any] struct {
	resp T
	err  error
}

func (i *Item[T, K]) handleLeaseGranted(
	ctx context.Context, key K,
	setError func(err error),
	setResponse func(resp T),
	keyStr string, cas uint64,
) {
	fillFn := i.filler(ctx, key)
	i.sess.AddNextCall(func() {
		fillResp, err := fillFn()
		if err != nil {
			setError(err)
			return
		}

		data, err := fillResp.Marshal()
		if err != nil {
			setError(err)
			return
		}
		setResponse(fillResp)

		i.pipeline.LeaseSet(keyStr, data, cas, memproxy.LeaseSetOptions{})
	})
}

// Get ...
func (i *Item[T, K]) Get(ctx context.Context, key K) func() (T, error) {
	keyStr := key.String()

	returnFn := func() (T, error) {
		i.sess.Execute()

		result := i.getKeys[key]
		return result.resp, result.err
	}

	_, existed := i.getKeys[key]
	if existed {
		return returnFn
	}
	i.getKeys[key] = getResultType[T]{}

	retryCount := 0

	leaseGetFn := i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})

	var nextFn func()

	setError := func(err error) {
		i.getKeys[key] = getResultType[T]{
			err: err,
		}
	}

	setResponse := func(resp T) {
		i.getKeys[key] = getResultType[T]{
			resp: resp,
		}
	}

	nextFn = func() {
		leaseGetResp, err := leaseGetFn()
		if err != nil {
			setError(err)
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusFound {
			resp, err := i.unmarshaler(leaseGetResp.Data)
			if err != nil {
				setError(err)
				return
			}
			setResponse(resp)
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseGranted {
			i.handleLeaseGranted(ctx, key,
				setError, setResponse,
				keyStr, leaseGetResp.CAS,
			)
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseRejected {
			if retryCount >= len(i.options.sleepDurations) {
				setError(ErrExceededRejectRetryLimit)
				return
			}

			i.sess.AddDelayedCall(i.options.sleepDurations[retryCount], func() {
				retryCount++

				leaseGetFn = i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})
				i.sess.AddNextCall(nextFn)
			})
			return
		}

		setError(ErrInvalidLeaseGetStatus)
	}

	i.sess.AddNextCall(nextFn)

	return returnFn
}

// LowerSession ...
func (i *Item[T, K]) LowerSession() memproxy.Session {
	return i.sess.GetLower()
}

// Reset clear in-memory cached values
func (i *Item[T, K]) Reset() {
	i.getKeys = map[K]getResultType[T]{}
}
