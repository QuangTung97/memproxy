package item

import (
	"context"
	"github.com/QuangTung97/memproxy"
)

// Value ...
type Value interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// Key ...
type Key interface {
	String() string
}

// Filler ...
type Filler[T, K any] interface {
	GetItem(ctx context.Context, key K) func() (T, error)
}

type fillerFunc[T, K any] struct {
	fn func(ctx context.Context, key K) func() (T, error)
}

func (f *fillerFunc[T, K]) GetItem(ctx context.Context, key K) func() (T, error) {
	return f.fn(ctx, key)
}

// FillerFunc ...
func FillerFunc[T, K any](fn func(ctx context.Context, key K) func() (T, error)) Filler[T, K] {
	return &fillerFunc[T, K]{fn: fn}
}

// NewOptions ...
type NewOptions struct {
}

// New ...
func New[T Value, K Key](
	sess memproxy.Session,
	pipeline memproxy.Pipeline,
	filler Filler[T, K],
	options NewOptions,
) *Item[T, K] {
	return &Item[T, K]{
		sess:     sess,
		pipeline: pipeline,
		filler:   filler,
	}
}

// Item ...
type Item[T Value, K Key] struct {
	sess     memproxy.Session
	pipeline memproxy.Pipeline
	filler   Filler[T, K]
}

// Get ...
func (i *Item[T, K]) Get(ctx context.Context, key K) func() (T, error) {
	keyStr := key.String()

	leaseGetFn := i.pipeline.LeaseGet(keyStr, memproxy.LeaseGetOptions{})

	type resultType struct {
		resp T
		err  error
	}

	var result resultType

	i.sess.AddNextCall(func() {
		leaseGetResp, err := leaseGetFn()
		if err != nil {
			result.err = err
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusFound {
			err := result.resp.Unmarshal(leaseGetResp.Data)
			if err != nil {
				result.err = err
			}
			return
		}

		if leaseGetResp.Status == memproxy.LeaseGetStatusLeaseGranted {
			fillFn := i.filler.GetItem(ctx, key)
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

				i.pipeline.LeaseSet(keyStr, data, leaseGetResp.CAS, memproxy.LeaseSetOptions{})
			})
		}
	})

	return func() (T, error) {
		i.sess.Execute()
		return result.resp, result.err
	}
}
