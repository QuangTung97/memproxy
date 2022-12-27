package memproxy

import (
	"context"
	"github.com/QuangTung97/go-memcache/memcache"
)

type plainMemcacheImpl struct {
	client        *memcache.Client
	leaseDuration uint32
}

var _ Memcache = &plainMemcacheImpl{}

type plainPipelineImpl struct {
	pipeline      *memcache.Pipeline
	leaseDuration uint32
}

var _ Pipeline = &plainPipelineImpl{}

// NewPlainMemcache a light wrapper around memcached client
func NewPlainMemcache(client *memcache.Client, leaseDuration uint32) Memcache {
	return &plainMemcacheImpl{
		client:        client,
		leaseDuration: leaseDuration,
	}
}

// Pipeline ...
func (m *plainMemcacheImpl) Pipeline(_ context.Context, _ Session, _ ...PipelineOption) Pipeline {
	return &plainPipelineImpl{
		pipeline:      m.client.Pipeline(),
		leaseDuration: m.leaseDuration,
	}
}

// Get ...
func (p *plainPipelineImpl) Get(key string, _ GetOptions) func() (GetResponse, error) {
	fn := p.pipeline.MGet(key, memcache.MGetOptions{})
	return func() (GetResponse, error) {
		resp, err := fn()
		if err != nil {
			return GetResponse{}, err
		}
		if resp.Type == memcache.MGetResponseTypeVA {
			return GetResponse{
				Found: true,
				Data:  resp.Data,
			}, nil
		}
		return GetResponse{}, nil
	}
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
		_, err := fn()
		if err != nil {
			return LeaseSetResponse{}, err
		}
		return LeaseSetResponse{}, nil
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
