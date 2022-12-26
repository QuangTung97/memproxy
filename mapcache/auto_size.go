package mapcache

import (
	"context"
	"fmt"
	"github.com/QuangTung97/memproxy"
)

type autoSizeProviderImpl struct {
	sizeLogClient memproxy.Memcache
	client        memproxy.Memcache
}

// NewAutoSizeProvider is thread safe
func NewAutoSizeProvider(
	client memproxy.Memcache, fillerFactory FillerFactory,
) AutoSizeProvider {
	return &autoSizeProviderImpl{
		sizeLogClient: memproxy.NewFillerMemcache(client, &fillerFactoryImpl{}),
		client:        memproxy.NewFillerMemcache(client, &fillerFactoryImpl{factory: fillerFactory}),
	}
}

type autoSizeMapCacheImpl struct {
	sizeLogPipeline memproxy.Pipeline
	origin          mapCacheImpl
}

// New ...
func (p *autoSizeProviderImpl) New(
	ctx context.Context, sess memproxy.Session,
	rootKey string, options NewOptions,
) MapCache {
	m := &autoSizeMapCacheImpl{
		sizeLogPipeline: p.sizeLogClient.Pipeline(ctx, sess),
	}
	initMapCacheImpl(ctx, &m.origin, p.client, sess, rootKey, options)

	return m
}

type autoSizeParams struct {
	resp GetResponse
	err  error
}

func (p *autoSizeParams) setError(err error) {
	p.resp = GetResponse{}
	p.err = err
}

// Get ...
func (m *autoSizeMapCacheImpl) Get(
	key string, options GetOptions,
) func() (GetResponse, error) {
	sizeLogFn := m.origin.pipeline.LeaseGet(m.origin.conf.rootKey+":size-log", memproxy.LeaseGetOptions{})

	params := &autoSizeParams{}
	m.origin.sess.AddNextCall(func() {
		sizeLogResp, err := sizeLogFn()
		if err != nil {
			params.setError(err)
			return
		}

		fmt.Println(sizeLogResp)

		m.origin.sess.AddNextCall(func() {
			fn := m.origin.Get(key, options)

			m.origin.sess.AddNextCall(func() {
				params.resp, params.err = fn()
			})
		})
	})

	return func() (GetResponse, error) {
		m.origin.sess.Execute()
		return params.resp, params.err
	}
}
