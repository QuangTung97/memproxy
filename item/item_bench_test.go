package item

import (
	"context"
	"encoding/binary"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"runtime"
	"strconv"
	"testing"
	"time"
)

type benchValue struct {
	key   uint64
	value int64
}

type benchKey struct {
	key uint64
}

func (k benchKey) String() string {
	return strconv.FormatUint(k.key, 10)
}

func (v benchValue) Marshal() ([]byte, error) {
	var result [16]byte
	binary.LittleEndian.PutUint64(result[:], v.key)
	binary.LittleEndian.PutUint64(result[8:], uint64(v.value))
	return result[:], nil
}

func unmarshalBench(data []byte) (benchValue, error) {
	key := binary.LittleEndian.Uint64(data[:])
	val := binary.LittleEndian.Uint64(data[8:])
	return benchValue{
		key:   key,
		value: int64(val),
	}, nil
}

func TestMarshalBench(t *testing.T) {
	v := benchValue{
		key:   124,
		value: 889,
	}

	data, err := v.Marshal()
	assert.Equal(t, nil, err)

	result, err := unmarshalBench(data)
	assert.Equal(t, nil, err)
	assert.Equal(t, v, result)
}

func clearMemcache(c *memcache.Client) {
	pipe := c.Pipeline()
	defer pipe.Finish()
	err := pipe.FlushAll()()
	if err != nil {
		panic(err)
	}
}

func newMemcache() (memproxy.Memcache, memproxy.SessionProvider) {
	client, err := memcache.New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	clearMemcache(client)

	mc := memproxy.NewPlainMemcache(client, 3)
	sess := memproxy.NewSessionProvider(time.Now, time.Sleep)
	return mc, sess
}

func BenchmarkItemGetSingle(b *testing.B) {
	mc, sess := newMemcache()

	b.ResetTimer()

	value := int64(112)

	for n := 0; n < b.N; n++ {
		pipe := mc.Pipeline(context.Background(), sess.New())

		var filler Filler[benchValue, benchKey] = func(ctx context.Context, key benchKey) func() (benchValue, error) {
			return func() (benchValue, error) {
				value++
				return benchValue{
					key:   key.key,
					value: value,
				}, nil
			}
		}
		autoFill := New[benchValue, benchKey](pipe, unmarshalBench, filler)

		fn := autoFill.Get(context.Background(), benchKey{
			key: 3344,
		})

		val, err := fn()
		if err != nil {
			panic(err)
		}

		if val.value != value {
			panic(value)
		}

		pipe.Finish()
	}
}

func benchmarkWithBatch(b *testing.B, batchSize int) {
	mc, sess := newMemcache()

	b.ResetTimer()

	value := int64(112)

	for n := 0; n < b.N; n++ {
		pipe := mc.Pipeline(context.Background(), sess.New())

		var filler Filler[benchValue, benchKey] = func(ctx context.Context, key benchKey) func() (benchValue, error) {
			return func() (benchValue, error) {
				value++
				return benchValue{
					key:   key.key,
					value: value,
				}, nil
			}
		}
		autoFill := New[benchValue, benchKey](pipe, unmarshalBench, filler)

		fnList := make([]func() (benchValue, error), 0, batchSize)
		for i := 0; i < batchSize; i++ {
			fn := autoFill.Get(context.Background(), benchKey{
				key: 33000 + uint64(i),
			})
			fnList = append(fnList, fn)
		}

		for _, fn := range fnList {
			_, err := fn()
			if err != nil {
				panic(err)
			}
		}
		pipe.Finish()
	}
}

func BenchmarkItemGetByBatch1000(b *testing.B) {
	benchmarkWithBatch(b, 1000) // => 400K / seconds
}

func BenchmarkItemGetByBatch100(b *testing.B) {
	benchmarkWithBatch(b, 100) // => 348K / seconds
}

func BenchmarkHeapAlloc(b *testing.B) {
	count := uint64(0)
	var last any
	for n := 0; n < b.N; n++ {
		x := make([]byte, 128)
		var v any = x
		v.([]byte)[0] = uint8(count)
		count += uint64(x[0])
		last = x
	}
	runtime.KeepAlive(last)
}
