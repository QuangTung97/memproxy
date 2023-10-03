package mmap

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRootKey struct {
	prefix string
}

func (k testRootKey) String() string {
	return k.prefix
}

// length in bytes
func newHash(prefix uint64, length int) uint64 {
	size := length * 8
	return prefix << (64 - size)

}

func TestNewHash(t *testing.T) {
	assert.Equal(t, uint64(0x1234_5600_0000_0000), newHash(0x1234_56, 3))
}

func TestBucketKey_String(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 3 * 8,
			Hash:    newHash(0x1234_5678, 4),
			Sep:     ":",
		}
		assert.Equal(t, "hello:24:123456", k.String())
	})

	t.Run("near align with bytes", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 23,
			Hash:    newHash(0x1234_ff78, 4),
			Sep:     "/",
		}
		assert.Equal(t, "hello/23/1234fe", k.String())
	})

	t.Run("middle of byte", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 12,
			Hash:    newHash(0x1234_ff78, 4),
			Sep:     "/",
		}
		assert.Equal(t, "hello/12/123", k.String())
	})

	t.Run("single digit", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 4,
			Hash:    newHash(0x5234_ff78, 4),
			Sep:     "/",
		}
		assert.Equal(t, "hello/4/5", k.String())
	})

	t.Run("single bit", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 1,
			Hash:    newHash(0xf4, 1),
			Sep:     "/",
		}
		assert.Equal(t, "hello/1/8", k.String())
	})

	t.Run("size log zero", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 0,
			Hash:    newHash(0xf4, 1),
			Sep:     "/",
		}
		assert.Equal(t, "hello/0/", k.String())
	})
}

type testUser struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (u testUser) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func unmarshalTestUser(data []byte) (testUser, error) {
	var u testUser
	err := json.Unmarshal(data, &u)
	return u, err
}

type simpleString string

func (s simpleString) Marshal() ([]byte, error) {
	return []byte(s), nil
}

func unmarshalSimpleString(data []byte) (simpleString, error) {
	return simpleString(data), nil
}

func TestNewBucketUnmarshaler(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		u1 := testUser{
			Name: "user01",
			Age:  81,
		}
		u2 := testUser{
			Name: "user02",
			Age:  82,
		}

		bucket := Bucket[testUser]{
			Values: []testUser{u1, u2},
		}

		data, err := bucket.Marshal()
		assert.Equal(t, nil, err)

		unmarshaler := NewBucketUnmarshaler[testUser](unmarshalTestUser)

		newBucket, err := unmarshaler(data)
		assert.Equal(t, nil, err)
		assert.Equal(t, bucket, newBucket)
	})

	t.Run("empty", func(t *testing.T) {
		bucket := Bucket[testUser]{}

		data, err := bucket.Marshal()
		assert.Equal(t, nil, err)

		unmarshaler := NewBucketUnmarshaler[testUser](unmarshalTestUser)

		newBucket, err := unmarshaler(data)
		assert.Equal(t, nil, err)

		bucket.Values = []testUser{}
		assert.Equal(t, bucket, newBucket)
	})

	t.Run("byte format", func(t *testing.T) {
		bucket := Bucket[simpleString]{
			Values: []simpleString{
				"ABC",
				"X",
			},
		}

		data, err := bucket.Marshal()
		assert.Equal(t, nil, err)
		assert.Equal(t, []byte{2, 3, 'A', 'B', 'C', 1, 'X'}, data)
	})

	t.Run("missing length", func(t *testing.T) {
		fn := NewBucketUnmarshaler[simpleString](unmarshalSimpleString)

		_, err := fn(nil)
		assert.Equal(t, errors.New("mmap bucket: invalid number of values"), err)
	})

	t.Run("missing data len", func(t *testing.T) {
		fn := NewBucketUnmarshaler[simpleString](unmarshalSimpleString)

		_, err := fn([]byte{2})
		assert.Equal(t, errors.New("mmap bucket: invalid length number of data"), err)
	})

	t.Run("missing data", func(t *testing.T) {
		fn := NewBucketUnmarshaler[simpleString](unmarshalSimpleString)

		_, err := fn([]byte{2, 3, 'A', 'B'})
		assert.Equal(t, errors.New("mmap bucket: invalid data"), err)
	})

	t.Run("success single record", func(t *testing.T) {
		fn := NewBucketUnmarshaler[simpleString](unmarshalSimpleString)

		bucket, err := fn([]byte{1, 3, 'A', 'B', 'C'})
		assert.Equal(t, nil, err)
		assert.Equal(t, Bucket[simpleString]{
			Values: []simpleString{
				"ABC",
			},
		}, bucket)
	})
}

func TestNewBucketUnmarshaler_WithInnerError(t *testing.T) {
	fn := NewBucketUnmarshaler[simpleString](func(data []byte) (simpleString, error) {
		return "", errors.New("inner error")
	})

	_, err := fn([]byte{1, 2, 'A', 'B'})
	assert.Equal(t, errors.New("inner error"), err)
}

type errorString string

func (s errorString) Marshal() ([]byte, error) {
	return nil, errors.New("marshal error")
}

func TestBucket_Marshal_With_Error(t *testing.T) {
	b := Bucket[errorString]{
		Values: []errorString{
			"ABCD",
		},
	}
	_, err := b.Marshal()
	assert.Equal(t, errors.New("marshal error"), err)
}
