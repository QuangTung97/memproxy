package memproxy

import (
	"context"
	"github.com/QuangTung97/go-memcache/memcache"
)

type plainMemcacheImpl struct {
	client        *memcache.Client
	sessProvider  SessionProvider
	leaseDuration uint32
}

var _ Memcache = &plainMemcacheImpl{}

type plainPipelineImpl struct {
	sess          Session
	pipeline      *memcache.Pipeline
	leaseDuration uint32
}

type plainMemcacheConfig struct {
	leaseDurationSeconds uint32
	sessProvider         SessionProvider
}

func computePlainMemcacheConfig(options ...PlainMemcacheOption) *plainMemcacheConfig {
	conf := &plainMemcacheConfig{
		leaseDurationSeconds: 3,
		sessProvider:         NewSessionProvider(),
	}
	for _, fn := range options {
		fn(conf)
	}
	return conf
}

// PlainMemcacheOption ...
type PlainMemcacheOption func(opts *plainMemcacheConfig)

// WithPlainMemcacheLeaseDuration ...
func WithPlainMemcacheLeaseDuration(leaseDurationSeconds uint32) PlainMemcacheOption {
	return func(opts *plainMemcacheConfig) {
		opts.leaseDurationSeconds = leaseDurationSeconds
	}
}

// WithPlainMemcacheSessionProvider ...
func WithPlainMemcacheSessionProvider(sessProvider SessionProvider) PlainMemcacheOption {
	return func(opts *plainMemcacheConfig) {
		opts.sessProvider = sessProvider
	}
}

var _ Pipeline = &plainPipelineImpl{}

// NewPlainMemcache a light wrapper around memcached client
func NewPlainMemcache(
	client *memcache.Client,
	options ...PlainMemcacheOption,
) Memcache {
	conf := computePlainMemcacheConfig(options...)
	return &plainMemcacheImpl{
		client:        client,
		sessProvider:  conf.sessProvider,
		leaseDuration: conf.leaseDurationSeconds,
	}
}

// Pipeline ...
func (m *plainMemcacheImpl) Pipeline(_ context.Context, options ...PipelineOption) Pipeline {
	conf := ComputePipelineConfig(options)
	sess := conf.GetSession(m.sessProvider)

	return &plainPipelineImpl{
		sess:          sess,
		pipeline:      m.client.Pipeline(),
		leaseDuration: m.leaseDuration,
	}
}

// Close ...
func (m *plainMemcacheImpl) Close() error {
	return m.client.Close()
}

func (p *plainPipelineImpl) LowerSession() Session {
	return p.sess.GetLower()
}

// LeaseGet ...
func (p *plainPipelineImpl) LeaseGet(key string, _ LeaseGetOptions) func() (LeaseGetResponse, error) {
	fn := p.pipeline.MGet(key, memcache.MGetOptions{
		N:   p.leaseDuration,
		CAS: true,
	})
	return func() (LeaseGetResponse, error) {
		mgetResp, err := fn()
		if err != nil {
			return LeaseGetResponse{}, err
		}

		if mgetResp.Type != memcache.MGetResponseTypeVA {
			return LeaseGetResponse{}, ErrInvalidLeaseGetResponse
		}

		if mgetResp.Flags == 0 {
			return LeaseGetResponse{
				Status: LeaseGetStatusFound,
				CAS:    mgetResp.CAS,
				Data:   mgetResp.Data,
			}, nil
		}

		if (mgetResp.Flags & memcache.MGetFlagW) > 0 {
			return LeaseGetResponse{
				Status: LeaseGetStatusLeaseGranted,
				CAS:    mgetResp.CAS,
			}, nil
		}

		return LeaseGetResponse{
			Status: LeaseGetStatusLeaseRejected,
			CAS:    mgetResp.CAS,
		}, nil
	}
}

// LeaseSet ...
func (p *plainPipelineImpl) LeaseSet(
	key string, data []byte, cas uint64, options LeaseSetOptions,
) func() (LeaseSetResponse, error) {
	fn := p.pipeline.MSet(key, data, memcache.MSetOptions{
		CAS: cas,
		TTL: options.TTL,
	})
	return func() (LeaseSetResponse, error) {
		resp, err := fn()
		if err != nil {
			return LeaseSetResponse{}, err
		}
		status := LeaseSetStatusNotStored
		if resp.Type == memcache.MSetResponseTypeHD {
			status = LeaseSetStatusStored
		}
		return LeaseSetResponse{
			status,
		}, nil
	}
}

// Delete ...
func (p *plainPipelineImpl) Delete(key string, _ DeleteOptions) func() (DeleteResponse, error) {
	fn := p.pipeline.MDel(key, memcache.MDelOptions{})
	return func() (DeleteResponse, error) {
		_, err := fn()
		return DeleteResponse{}, err
	}
}

// Execute ...
func (p *plainPipelineImpl) Execute() {
	p.pipeline.Execute()
}

// Finish ...
func (p *plainPipelineImpl) Finish() {
	p.pipeline.Finish()
}
