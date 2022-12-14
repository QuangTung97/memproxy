package memproxy

import (
	"context"
	"time"
)

type fillerMemcacheImpl struct {
	origin  Memcache
	factory FillerFactory
}

var _ Memcache = &fillerMemcacheImpl{}

// NewFillerMemcache protects again then *Thundering Herb Problem*
// With this Decorator, status = LeaseGetStatusLeaseGranted or status = LeaseGetStatusLeaseRejected will never happen
func NewFillerMemcache(origin Memcache, fillerFactory FillerFactory) Memcache {
	return &fillerMemcacheImpl{
		origin:  origin,
		factory: fillerFactory,
	}
}

type fillerPipelineImpl struct {
	Pipeline

	ctx    context.Context
	sess   Session
	filler Filler
}

var _ Pipeline = &fillerPipelineImpl{}

// Pipeline ...
func (m *fillerMemcacheImpl) Pipeline(ctx context.Context, sess Session, options ...PipelineOption) Pipeline {
	opts := computePipelineOptions(options)
	return &fillerPipelineImpl{
		Pipeline: m.origin.Pipeline(ctx, sess),

		ctx:    ctx,
		sess:   sess,
		filler: m.factory.New(sess, opts.newFillerParams),
	}
}

// LeaseGet ...
func (p *fillerPipelineImpl) LeaseGet(key string, options LeaseGetOptions) func() (LeaseGetResponse, error) {
	fn := p.Pipeline.LeaseGet(key, options)

	var resp LeaseGetResponse
	var err error

	retryCount := 0

	const baseDuration = 5 * time.Millisecond
	const multiplicativeFactor = 4
	const retryLimit = 5

	sleepDuration := baseDuration

	var nextFn func()
	nextFn = func() {
		resp, err = fn()
		if err != nil {
			return
		}
		if resp.Status == LeaseGetStatusFound {
			return
		}

		if resp.Status == LeaseGetStatusLeaseGranted {
			completeFn := func(fillResp FillResponse, fillErr error) {
				err = fillErr
				if err != nil {
					resp = LeaseGetResponse{}
					p.Pipeline.Delete(key, DeleteOptions{})
					return
				}

				p.Pipeline.LeaseSet(key, fillResp.Data, resp.CAS, LeaseSetOptions{})

				resp = LeaseGetResponse{
					Status: LeaseGetStatusFound,
					CAS:    0,
					Data:   fillResp.Data,
				}
			}
			p.filler.Fill(p.ctx, options.FillParams, completeFn)
			return
		}

		retryCount++
		if retryCount > retryLimit {
			resp = LeaseGetResponse{}
			err = ErrExceededRejectRetryLimit
			return
		}

		fn = p.Pipeline.LeaseGet(key, options)

		p.sess.AddDelayedCall(sleepDuration, nextFn)
		sleepDuration = sleepDuration * multiplicativeFactor
	}
	p.sess.AddNextCall(nextFn)

	return func() (LeaseGetResponse, error) {
		p.sess.Execute()
		return resp, err
	}
}
