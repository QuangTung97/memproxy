package mmap

import (
	"context"
	"encoding/binary"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
)

type benchValue struct {
	rootKey benchRootKey
	key     benchKey
	value   int64
}

type benchRootKey struct {
	value uint64
}

type benchKey struct {
	value uint64
}

const benchKeyNum = 229
const benchValueNum = 331

func newBenchMapCache(pipe memproxy.Pipeline) *Map[benchValue, benchRootKey, benchKey] {
	return New[benchValue, benchRootKey, benchKey](
		pipe,
		unmarshalBenchValue,
		func(ctx context.Context, rootKey benchRootKey, hashRange HashRange) func() ([]benchValue, error) {
			return func() ([]benchValue, error) {
				return []benchValue{
					{
						rootKey: rootKey,
						key: benchKey{
							value: benchKeyNum,
						},
						value: benchValueNum,
					},
				}, nil
			}
		},
		benchValue.getKey,
	)
}

func doGetMapElemFromMemcache(mc memproxy.Memcache, numKeys int) {
	pipe := mc.Pipeline(context.Background())
	defer pipe.Finish()

	mapCache := newBenchMapCache(pipe)

	fnList := make([]func() (Option[benchValue], error), 0, numKeys)
	for i := 0; i < numKeys; i++ {
		fn := mapCache.Get(context.Background(), uint64(numKeys), benchRootKey{
			value: uint64(1000 + i),
		}, benchKey{
			value: benchKeyNum,
		})
		fnList = append(fnList, fn)
	}

	for _, fn := range fnList {
		result, err := fn()
		if err != nil {
			panic(err)
		}
		if !result.Valid {
			panic("not valid")
		}
		if result.Data.value != benchValueNum {
			panic("wrong value")
		}
	}
}

func BenchmarkWithProxy__Map_Get_Batch_100(b *testing.B) {
	mc := newMemcacheWithProxy(b)

	const numKeys = 100

	doGetMapElemFromMemcache(mc, numKeys)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		doGetMapElemFromMemcache(mc, numKeys)
	}

	b.StopTimer()
	writeMemProfile()
}

func BenchmarkWithProxy__Map_Get_Batch_1000(b *testing.B) {
	mc := newMemcacheWithProxy(b)

	const numKeys = 1000

	doGetMapElemFromMemcache(mc, numKeys)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		doGetMapElemFromMemcache(mc, numKeys)
	}

	b.StopTimer()
	writeMemProfile()
}

func BenchmarkComputeBucketKeyString(b *testing.B) {
	var sum int
	for n := 0; n < b.N; n++ {
		k := BucketKey[benchRootKey]{
			RootKey: benchRootKey{
				value: 23,
			},
			SizeLog: 7,
			Hash:    newHash(0x1234, 2),
			Sep:     ":",
		}.String()
		sum += len(k)
	}
	b.StopTimer()
	writeMemProfile()
}

func (v benchValue) getKey() benchKey {
	return v.key
}

func (k benchKey) Hash() uint64 {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], k.value)
	return murmur3.Sum64(data[:])
}

func (k benchRootKey) String() string {
	return strconv.FormatUint(k.value, 10)
}

func (benchRootKey) AvgBucketSizeLog() uint8 {
	return 1
}

func (v benchValue) Marshal() ([]byte, error) {
	var result [24]byte
	binary.LittleEndian.PutUint64(result[:], v.rootKey.value)
	binary.LittleEndian.PutUint64(result[8:], v.key.value)
	binary.LittleEndian.PutUint64(result[16:], uint64(v.value))
	return result[:], nil
}

func unmarshalBenchValue(data []byte) (benchValue, error) {
	rootKey := binary.LittleEndian.Uint64(data[:])
	key := binary.LittleEndian.Uint64(data[8:])
	val := binary.LittleEndian.Uint64(data[16:])

	return benchValue{
		rootKey: benchRootKey{
			value: rootKey,
		},
		key: benchKey{
			value: key,
		},
		value: int64(val),
	}, nil
}

func TestMarshalBenchValue(t *testing.T) {
	b := benchValue{
		rootKey: benchRootKey{
			value: 41,
		},
		key: benchKey{
			value: 31,
		},
		value: 55,
	}
	data, err := b.Marshal()
	assert.Equal(t, nil, err)

	newVal, err := unmarshalBenchValue(data)
	assert.Equal(t, nil, err)
	assert.Equal(t, b, newVal)
}

func writeMemProfile() {
	if os.Getenv("ENABLE_BENCH_PROFILE") == "" {
		return
	}

	file, err := os.Create("./bench_profile.out")
	if err != nil {
		panic(err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}()

	err = pprof.WriteHeapProfile(file)
	if err != nil {
		panic(err)
	}
}
