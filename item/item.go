package item

import (
	"context"
	"github.com/QuangTung97/memproxy"
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

// New ...
func New[T Value, K Key](
	sess memproxy.Session,
	pipeline memproxy.Pipeline,

	unmarshaler Unmarshaler[T],
	filler Filler[T, K],
) *Item[T, K] {
	return &Item[T, K]{
		sess:     sess,
		pipeline: pipeline,

		unmarshaler: unmarshaler,
		filler:      filler,
	}
}

// Item ...
type Item[T Value, K Key] struct {
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

	i.sess.AddNextCall(func() {
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
	})

	return func() (T, error) {
		i.sess.Execute()
		return result.resp, result.err
	}
}
