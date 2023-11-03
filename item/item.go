package item

import (
	"context"
	"errors"
	"log"
	"time"
	"unsafe"

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
		common: itemCommon{
			options:  computeOptions(options),
			sess:     pipeline.LowerSession(),
			pipeline: pipeline,
		},

		unmarshaler: unmarshaler,
		filler:      filler,

		getKeys: map[K]*getResultType[T]{},
	}
}

// Item is NOT thread safe and, it contains a cached keys
// once a key is cached in memory, it will return the same value unless call **Reset**
type Item[T Value, K Key] struct {
	unmarshaler Unmarshaler[T]
	filler      Filler[T, K]

	getKeys map[K]*getResultType[T]

	common itemCommon
}

type itemCommon struct {
	options  *itemOptions
	sess     memproxy.Session
	pipeline memproxy.Pipeline
	stats    Stats
}

func (i *itemCommon) addNextCall(fn func(obj unsafe.Pointer)) {
	i.sess.AddNextCall(memproxy.CallbackFunc{
		Object: nil,
		Func:   fn,
	})
}

func (i *itemCommon) addDelayedCall(d time.Duration, fn func(obj unsafe.Pointer)) {
	i.sess.AddDelayedCall(d, memproxy.CallbackFunc{
		Object: nil,
		Func:   fn,
	})
}

type getResultType[T any] struct {
	resp T
	err  error
}

func (s *GetState[T, K]) handleLeaseGranted(cas uint64) {
	it := s.getItem()

	fillFn := it.filler(s.common.ctx, s.key)

	it.common.addNextCall(func(_ unsafe.Pointer) {
		it := s.common.item

		fillResp, err := fillFn()

		if err == ErrNotFound {
			s.setResponse(fillResp)
			it.pipeline.Delete(s.common.keyStr, memproxy.DeleteOptions{})
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
			_ = it.pipeline.LeaseSet(s.common.keyStr, data, cas, memproxy.LeaseSetOptions{})
			it.addNextCall(func(obj unsafe.Pointer) {
				s.common.item.pipeline.Execute()
			})
		}
	})
}

type getStateMethods interface {
	setResponseError(err error)
	doFillFunc(cas uint64)
	unmarshalAndSet(data []byte)
}

type getStateCommon struct {
	ctx context.Context

	itemRoot unsafe.Pointer
	item     *itemCommon

	retryCount int
	keyStr     string

	leaseGetResult memproxy.LeaseGetResult

	methods getStateMethods
}

// GetState store intermediate state when getting item
type GetState[T Value, K Key] struct {
	common *getStateCommon
	key    K
	result getResultType[T]
}

func (s *GetState[T, K]) getItem() *Item[T, K] {
	return (*Item[T, K])(s.common.itemRoot)
}

func (s *GetState[T, K]) unmarshalAndSet(data []byte) {
	it := s.getItem()
	resp, err := it.unmarshaler(data)

	memcache.ReleaseGetResponseData(data)

	if err != nil {
		s.setResponseError(err)
		return
	}
	s.setResponse(resp)
}

func (s *GetState[T, K]) setResponseError(err error) {
	it := s.getItem()

	s.common.item.options.errorLogger(err)
	it.getKeys[s.key].err = err
}

func (s *GetState[T, K]) setResponse(resp T) {
	it := s.getItem()
	it.getKeys[s.key].resp = resp
}

func (s *GetState[T, K]) doFillFunc(cas uint64) {
	s.common.item.stats.FillCount++
	s.handleLeaseGranted(cas)
}

func (s *getStateCommon) handleCacheError(err error) {
	s.item.stats.LeaseGetError++
	if s.item.options.fillingOnCacheError {
		s.item.options.errorLogger(err)
		s.methods.doFillFunc(0)
	} else {
		s.methods.setResponseError(err)
	}
}

func (s *getStateCommon) newNextCallback() memproxy.CallbackFunc {
	return memproxy.CallbackFunc{
		Object: unsafe.Pointer(s),
		Func:   stateCommonNextCallback,
	}
}

func stateCommonNextCallback(obj unsafe.Pointer) {
	s := (*getStateCommon)(obj)
	s.nextFunc()
}

func (s *getStateCommon) nextFunc() {
	leaseGetResp, err := s.leaseGetResult.Result()

	s.leaseGetResult = nil

	if err != nil {
		s.handleCacheError(err)
		return
	}

	it := s.item

	if leaseGetResp.Status == memproxy.LeaseGetStatusFound {
		it.stats.HitCount++
		it.stats.TotalBytesRecv += uint64(len(leaseGetResp.Data))

		s.methods.unmarshalAndSet(leaseGetResp.Data)
		return
	}

	if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseGranted {
		s.methods.doFillFunc(leaseGetResp.CAS)
		return
	}

	if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseRejected {
		it.increaseRejectedCount(s.retryCount)

		if s.retryCount < len(it.options.sleepDurations) {
			it.addDelayedCall(it.options.sleepDurations[s.retryCount], func(_ unsafe.Pointer) {
				s.retryCount++

				s.leaseGetResult = it.pipeline.LeaseGet(s.keyStr, memproxy.LeaseGetOptions{})
				it.sess.AddNextCall(s.newNextCallback())
			})
			return
		}

		if !it.options.errorOnRetryLimit {
			s.methods.doFillFunc(leaseGetResp.CAS)
			return
		}

		s.methods.setResponseError(ErrExceededRejectRetryLimit)
		return
	}

	s.handleCacheError(ErrInvalidLeaseGetStatus)
}

// Result returns result
func (s *GetState[T, K]) Result() (T, error) {
	it := s.getItem()
	it.common.sess.Execute()

	putGetStateCommon(s.common)
	s.common = nil

	result := it.getKeys[s.key]
	return result.resp, result.err
}

// Get a single item with key
func (i *Item[T, K]) Get(ctx context.Context, key K) func() (T, error) {
	return i.GetFast(ctx, key).Result
}

// GetFast is similar to Get but reduced one alloc
func (i *Item[T, K]) GetFast(ctx context.Context, key K) *GetState[T, K] {
	keyStr := key.String()

	// init get state common
	sc := newGetStateCommon()

	sc.ctx = ctx

	sc.itemRoot = unsafe.Pointer(i)
	sc.item = &i.common

	sc.keyStr = keyStr
	// end init get state common

	state := &GetState[T, K]{
		common: sc,
		key:    key,
	}

	sc.methods = state

	_, existed := i.getKeys[key]
	if existed {
		return state
	}
	i.getKeys[key] = &state.result

	sc.leaseGetResult = i.common.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})

	i.common.sess.AddNextCall(sc.newNextCallback())

	return state
}

// GetMulti gets multiple keys at once
func (i *Item[T, K]) GetMulti(ctx context.Context, keys []K) func() ([]T, error) {
	states := make([]*GetState[T, K], 0, len(keys))
	for _, k := range keys {
		state := i.GetFast(ctx, k)
		states = append(states, state)
	}

	return func() ([]T, error) {
		result := make([]T, 0, len(states))
		for _, state := range states {
			val, err := state.Result()
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil
	}
}

func (i *itemCommon) increaseRejectedCount(retryCount int) {
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
	return i.common.sess.GetLower()
}

// Reset clear in-memory cached values
func (i *Item[T, K]) Reset() {
	i.getKeys = map[K]*getResultType[T]{}
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
	return i.common.stats
}
