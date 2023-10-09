package item

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/QuangTung97/go-memcache/memcache"

	"github.com/QuangTung97/memproxy"
)

// Value is the value constraint
type Value interface {
	Marshal() ([]byte, error)
}

// Key is the key constraint
type Key interface {
	comparable
	String() string
}

// Unmarshaler transforms raw bytes from memcached servers to the correct type
type Unmarshaler[T any] func(data []byte) (T, error)

// Filler is for getting data from the backing store and set back to memcached servers
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

// WithSleepDurations configures the sleep durations and number of retries after Lease Get returns Rejected status
// default is the value of DefaultSleepDurations()
func WithSleepDurations(durations ...time.Duration) Option {
	return func(opts *itemOptions) {
		opts.sleepDurations = durations
	}
}

// WithEnableErrorOnExceedRetryLimit enables returning error if sleepDurations exceeded
// when enable = true, and after retried all the durations configured by WithSleepDurations,
// the Item.Get will return the error ErrExceededRejectRetryLimit
// default enable = false
func WithEnableErrorOnExceedRetryLimit(enable bool) Option {
	return func(opts *itemOptions) {
		opts.errorOnRetryLimit = enable
	}
}

// WithEnableFillingOnCacheError when enable = true, continue to read from DB when get from memcached returns error
// default enable = false
func WithEnableFillingOnCacheError(enable bool) Option {
	return func(opts *itemOptions) {
		opts.fillingOnCacheError = enable
	}
}

// WithErrorLogger configures the error logger when there are problems with the memcache client or unmarshalling
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
var ErrInvalidLeaseGetStatus = errors.New("item: invalid lease get response status")

type multiGetState[T any, K comparable] struct {
	keys   []K
	result map[K]T
	err    error
}

type multiGetFillerConfig struct {
	deleteOnNotFound bool
}

// MultiGetFillerOption ...
type MultiGetFillerOption func(conf *multiGetFillerConfig)

// WithMultiGetEnableDeleteOnNotFound when enable = true will delete the empty
// key-value (used for lease get) from memcached server,
// when the multiGetFunc NOT returning the corresponding values for the keys.
// Otherwise, the empty value (zero value) will be set to the memcached server.
// By default, enable = false.
func WithMultiGetEnableDeleteOnNotFound(enable bool) MultiGetFillerOption {
	return func(conf *multiGetFillerConfig) {
		conf.deleteOnNotFound = enable
	}
}

// NewMultiGetFiller ...
//
//revive:disable-next-line:cognitive-complexity
func NewMultiGetFiller[T any, K comparable](
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

// New creates an item.Item.
// Param: unmarshaler is for unmarshalling the Value type.
// Param: filler is for fetching data from the backing source (e.g. Database),
// and can use the function NewMultiGetFiller() for simple multi get from database
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

	stats Stats
}

type getResultType[T any] struct {
	resp T
	err  error
}

func (s *getState[T, K]) handleLeaseGranted(cas uint64) {
	fillFn := s.it.filler(s.ctx, s.key)
	s.it.sess.AddNextCall(func() {
		fillResp, err := fillFn()

		if err == ErrNotFound {
			s.setResponse(fillResp)
			s.it.pipeline.Delete(s.keyStr, memproxy.DeleteOptions{})
			return
		}

		if err != nil {
			s.setResponseError(err)
			return
		}

		data, err := fillResp.Marshal()
		if err != nil {
			s.setResponseError(err)
			return
		}
		s.setResponse(fillResp)

		if cas > 0 {
			_ = s.it.pipeline.LeaseSet(s.keyStr, data, cas, memproxy.LeaseSetOptions{})
			s.it.sess.AddNextCall(s.it.pipeline.Execute)
		}
	})
}

type getState[T Value, K Key] struct {
	ctx context.Context
	key K

	it *Item[T, K]

	retryCount   int
	keyStr       string
	leaseGetFunc func() (memproxy.LeaseGetResponse, error)
}

func (s *getState[T, K]) setResponseError(err error) {
	s.it.options.errorLogger(err)
	s.it.getKeys[s.key] = getResultType[T]{
		err: err,
	}
}

func (s *getState[T, K]) setResponse(resp T) {
	s.it.getKeys[s.key] = getResultType[T]{
		resp: resp,
	}
}

func (s *getState[T, K]) doFillFunc(cas uint64) {
	s.it.stats.FillCount++
	s.handleLeaseGranted(cas)
}

func (s *getState[T, K]) handleCacheError(err error) {
	s.it.stats.LeaseGetError++
	if s.it.options.fillingOnCacheError {
		s.it.options.errorLogger(err)
		s.doFillFunc(0)
	} else {
		s.setResponseError(err)
	}
}

func (s *getState[T, K]) nextFunc() {
	leaseGetResp, err := s.leaseGetFunc()
	if err != nil {
		s.handleCacheError(err)
		return
	}

	if leaseGetResp.Status == memproxy.LeaseGetStatusFound {
		s.it.stats.HitCount++
		s.it.stats.TotalBytesRecv += uint64(len(leaseGetResp.Data))

		resp, err := s.it.unmarshaler(leaseGetResp.Data)

		memcache.ReleaseGetResponseData(leaseGetResp.Data)

		if err != nil {
			s.setResponseError(err)
			return
		}
		s.setResponse(resp)
		return
	}

	if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseGranted {
		s.doFillFunc(leaseGetResp.CAS)
		return
	}

	if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseRejected {
		s.it.increaseRejectedCount(s.retryCount)

		if s.retryCount < len(s.it.options.sleepDurations) {
			s.it.sess.AddDelayedCall(s.it.options.sleepDurations[s.retryCount], func() {
				s.retryCount++

				s.leaseGetFunc = s.it.pipeline.LeaseGet(s.keyStr, memproxy.LeaseGetOptions{})
				s.it.sess.AddNextCall(s.nextFunc)
			})
			return
		}

		if !s.it.options.errorOnRetryLimit {
			s.doFillFunc(leaseGetResp.CAS)
			return
		}

		s.setResponseError(ErrExceededRejectRetryLimit)
		return
	}

	s.handleCacheError(ErrInvalidLeaseGetStatus)
}

func (s *getState[T, K]) returnFunc() (T, error) {
	s.it.sess.Execute()

	result := s.it.getKeys[s.key]
	return result.resp, result.err
}

// Get a single item with key
func (i *Item[T, K]) Get(ctx context.Context, key K) func() (T, error) {
	keyStr := key.String()

	state := &getState[T, K]{
		ctx: ctx,
		key: key,

		it: i,

		retryCount: 0,
		keyStr:     keyStr,
	}

	_, existed := i.getKeys[key]
	if existed {
		return state.returnFunc
	}
	i.getKeys[key] = getResultType[T]{}

	state.leaseGetFunc = i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})

	i.sess.AddNextCall(state.nextFunc)

	return state.returnFunc
}

func (i *Item[T, K]) increaseRejectedCount(retryCount int) {
	i.stats.TotalRejectedCount++

	switch retryCount {
	case 0:
		i.stats.FirstRejectedCount++
	case 1:
		i.stats.SecondRejectedCount++
	case 2:
		i.stats.ThirdRejectedCount++
	}
}

// LowerSession ...
func (i *Item[T, K]) LowerSession() memproxy.Session {
	return i.sess.GetLower()
}

// Reset clear in-memory cached values
func (i *Item[T, K]) Reset() {
	i.getKeys = map[K]getResultType[T]{}
}

// Stats ...
type Stats struct {
	HitCount  uint64
	FillCount uint64 // can also be interpreted as the miss count

	LeaseGetError uint64 // lease get error count

	FirstRejectedCount  uint64
	SecondRejectedCount uint64
	ThirdRejectedCount  uint64
	TotalRejectedCount  uint64

	TotalBytesRecv uint64
}

// GetStats ...
func (i *Item[T, K]) GetStats() Stats {
	return i.stats
}
