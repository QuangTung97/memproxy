package item

import (
	"context"
	"errors"
	"github.com/QuangTung97/memproxy"
	"log"
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
	sleepDurations      []time.Duration
	errorOnRetryLimit   bool
	fillingOnCacheError bool
	errorLogger         func(err error)
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
	}
}

func defaultErrorLogger(err error) {
	log.Println("[ERROR] item: get error:", err)
}

func computeOptions(options []Option) *itemOptions {
	opts := &itemOptions{
		sleepDurations:      DefaultSleepDurations(),
		errorOnRetryLimit:   false,
		fillingOnCacheError: false,
		errorLogger:         defaultErrorLogger,
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

// WithEnableErrorOnExceedRetryLimit ...
func WithEnableErrorOnExceedRetryLimit(enable bool) Option {
	return func(opts *itemOptions) {
		opts.errorOnRetryLimit = enable
	}
}

// WithEnableFillingOnCacheError continue to read from DB when get from memcached returns error
func WithEnableFillingOnCacheError(enable bool) Option {
	return func(opts *itemOptions) {
		opts.fillingOnCacheError = enable
	}
}

// WithErrorLogger ...
func WithErrorLogger(logger func(err error)) Option {
	return func(opts *itemOptions) {
		opts.errorLogger = logger
	}
}

// ErrNotFound ONLY be returned from the filler function, to do delete of lease get key in the memcached server
var ErrNotFound = errors.New("item: not found")

// ErrExceededRejectRetryLimit returned when number of rejected lease gets exceed number of sleep durations
var ErrExceededRejectRetryLimit = errors.New("item: exceeded lease rejected retry limit")

// ErrInvalidLeaseGetStatus ...
var ErrInvalidLeaseGetStatus = errors.New("item: exceeded lease get response status")

type multiGetState[T Value, K Key] struct {
	keys   []K
	result map[K]T
	err    error
}

type multiGetFillerConfig struct {
	deleteOnNotFound bool
}

// MultiGetFillerOption ...
type MultiGetFillerOption func(conf *multiGetFillerConfig)

// WithMultiGetEnableDeleteOnNotFound ...
func WithMultiGetEnableDeleteOnNotFound(enable bool) MultiGetFillerOption {
	return func(conf *multiGetFillerConfig) {
		conf.deleteOnNotFound = enable
	}
}

// NewMultiGetFiller ...
//
//revive:disable-next-line:cognitive-complexity
func NewMultiGetFiller[T Value, K Key](
	multiGetFunc func(ctx context.Context, keys []K) ([]T, error),
	getKey func(v T) K,
	options ...MultiGetFillerOption,
) Filler[T, K] {
	conf := &multiGetFillerConfig{
		deleteOnNotFound: false,
	}
	for _, opt := range options {
		opt(conf)
	}

	var state *multiGetState[T, K]

	return func(ctx context.Context, key K) func() (T, error) {
		if state == nil {
			state = &multiGetState[T, K]{
				result: map[K]T{},
			}
		}
		s := state
		s.keys = append(s.keys, key)

		return func() (T, error) {
			if state != nil {
				state = nil

				values, err := multiGetFunc(ctx, s.keys)
				if err != nil {
					s.err = err
				} else {
					for _, v := range values {
						s.result[getKey(v)] = v
					}
				}
			}

			if s.err != nil {
				var empty T
				return empty, s.err
			}

			result, ok := s.result[key]
			if !ok && conf.deleteOnNotFound {
				var empty T
				return empty, ErrNotFound
			}
			return result, nil
		}
	}
}

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

		if err == ErrNotFound {
			setResponse(fillResp)
			i.pipeline.Delete(keyStr, memproxy.DeleteOptions{})
			return
		}

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

		if cas > 0 {
			i.pipeline.LeaseSet(keyStr, data, cas, memproxy.LeaseSetOptions{})
		}
	})
}

// Get ...
//
//revive:disable-next-line:cognitive-complexity
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

	setResponseError := func(err error) {
		i.options.errorLogger(err)
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

		doFillFunc := func() {
			i.handleLeaseGranted(
				ctx, key,
				setResponseError, setResponse,
				keyStr, leaseGetResp.CAS,
			)
		}

		handleCacheError := func(err error) {
			if i.options.fillingOnCacheError {
				leaseGetResp = memproxy.LeaseGetResponse{}
				i.options.errorLogger(err)
				doFillFunc()
			} else {
				setResponseError(err)
			}
		}

		if err != nil {
			handleCacheError(err)
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusFound {
			resp, err := i.unmarshaler(leaseGetResp.Data)
			if err != nil {
				setResponseError(err)
				return
			}
			setResponse(resp)
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseGranted {
			doFillFunc()
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseRejected {
			if retryCount < len(i.options.sleepDurations) {
				i.sess.AddDelayedCall(i.options.sleepDurations[retryCount], func() {
					retryCount++

					leaseGetFn = i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})
					i.sess.AddNextCall(nextFn)
				})
				return
			}

			if !i.options.errorOnRetryLimit {
				doFillFunc()
				return
			}

			setResponseError(ErrExceededRejectRetryLimit)
			return
		}

		handleCacheError(ErrInvalidLeaseGetStatus)
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
