package item

import (
	"context"
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/proxy"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type itemPropertyTest struct {
	client *memcache.Client

	mc           memproxy.Memcache
	sessProvider memproxy.SessionProvider

	mut        sync.Mutex
	currentAge int64
}

func (p *itemPropertyTest) newItem() (*Item[userValue, userKey], func()) {
	pipe := p.mc.Pipeline(newContext(), p.sessProvider.New())
	return New[userValue, userKey](
		pipe, unmarshalUser,
		NewMultiGetFiller[userValue, userKey](func(ctx context.Context, keys []userKey) ([]userValue, error) {
			values := make([]userValue, 0, len(keys))

			p.mut.Lock()
			for _, k := range keys {
				values = append(values, userValue{
					Tenant: k.Tenant,
					Name:   k.Name,
					Age:    p.currentAge,
				})
			}
			p.mut.Unlock()

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(6)))

			return values, nil
		}, userValue.GetKey),
		WithEnableErrorOnExceedRetryLimit(true),
	), pipe.Finish
}

func (p *itemPropertyTest) updateAge(key userKey) {
	p.mut.Lock()
	p.currentAge++
	p.mut.Unlock()

	pipe := p.mc.Pipeline(newContext(), p.sessProvider.New())
	pipe.Delete(key.String(), memproxy.DeleteOptions{})
	pipe.Finish()
}

func (p *itemPropertyTest) flushAll() {
	pipe := p.client.Pipeline()
	err := pipe.FlushAll()()
	if err != nil {
		panic(err)
	}
}

func newItemPropertyTest(t *testing.T) *itemPropertyTest {
	p := &itemPropertyTest{}

	client, err := memcache.New("localhost:11211", 3)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	p.client = client

	p.mc = memproxy.NewPlainMemcache(client, 3)
	p.sessProvider = memproxy.NewSessionProvider()

	return p
}

func newItemPropertyTestWithProxy(t *testing.T) *itemPropertyTest {
	p := &itemPropertyTest{}

	client, err := memcache.New("localhost:11211", 3)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	p.client = client

	servers := []proxy.SimpleServerConfig{
		{
			Host: "localhost",
			Port: 11211,
		},
	}
	mc, closeFunc, err := proxy.NewSimpleReplicatedMemcache(
		servers, 3,
		proxy.NewSimpleStats(servers),
	)
	if err != nil {
		panic(err)
	}
	t.Cleanup(closeFunc)
	p.mc = mc

	p.sessProvider = memproxy.NewSessionProvider()

	return p
}

func (p *itemPropertyTest) testConsistency(t *testing.T) {
	var wg sync.WaitGroup

	const numThreads = 5

	wg.Add(numThreads * 4)

	for th := 0; th < numThreads*3; th++ {
		go func() {
			defer wg.Done()

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))

			it, finish := p.newItem()
			defer finish()

			fn := it.Get(newContext(), userKey{
				Tenant: "TENANT01",
				Name:   "user01",
			})
			_, err := fn()
			if err != nil {
				panic(err)
			}
		}()
	}

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))

			p.updateAge(userKey{
				Tenant: "TENANT01",
				Name:   "user01",
			})
		}()
	}

	wg.Wait()

	it, finish := p.newItem()
	defer finish()

	fn := it.Get(newContext(), userKey{
		Tenant: "TENANT01",
		Name:   "user01",
	})

	val, err := fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, userValue{
		Tenant: "TENANT01",
		Name:   "user01",
		Age:    p.currentAge,
	}, val)
	fmt.Println(p.currentAge)
}

func TestProperty_SingleKey(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		seed := time.Now().UnixNano()
		rand.Seed(seed)
		fmt.Println("SEED:", seed)

		p := newItemPropertyTest(t)

		for i := 0; i < 100; i++ {
			p.flushAll()
			p.testConsistency(t)
		}
	})

	t.Run("with-proxy", func(t *testing.T) {
		seed := time.Now().UnixNano()
		rand.Seed(seed)
		fmt.Println("SEED:", seed)

		p := newItemPropertyTestWithProxy(t)

		for i := 0; i < 100; i++ {
			p.flushAll()
			p.testConsistency(t)
		}
	})
}
