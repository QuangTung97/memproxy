package mhash

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type userTest struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (u userTest) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func TestBucketMarshaler(t *testing.T) {
	origin := Bucket[userTest]{
		NextLevel:       3,
		NextLevelPrefix: 0x7788 << (64 - 2*8),

		Items: []userTest{
			{
				ID:   21,
				Name: "USER01",
				Age:  51,
			},
			{
				ID:   22,
				Name: "USER02",
				Age:  52,
			},
		},
		Bitset: BitSet{4, 5, 6},
	}

	data, err := origin.Marshal()
	assert.Equal(t, nil, err)

	fmt.Println("DATA:", data)

	unmarshaler := func(data []byte) (userTest, error) {
		var u userTest
		err := json.Unmarshal(data, &u)
		return u, err
	}

	bucketUnmarshaler := BucketUnmarshalerFromItem[userTest](unmarshaler)
	newBucket, err := bucketUnmarshaler(data)

	assert.Equal(t, nil, err)
	assert.Equal(t, origin, newBucket)
}

func TestBucketMarshaler_Unmarshal_Empty(t *testing.T) {
	unmarshaler := func(data []byte) (userTest, error) {
		var u userTest
		err := json.Unmarshal(data, &u)
		return u, err
	}

	bucketUnmarshaler := BucketUnmarshalerFromItem[userTest](unmarshaler)
	newBucket, err := bucketUnmarshaler(nil)

	assert.Equal(t, nil, err)
	assert.Equal(t, Bucket[userTest]{}, newBucket)
}

func TestBucketUnmarshaler(t *testing.T) {
	unmarshaler := func(data []byte) (userTest, error) {
		var u userTest
		err := json.Unmarshal(data, &u)
		return u, err
	}
	bucketUnmarshaler := BucketUnmarshalerFromItem[userTest](unmarshaler)

	t.Run("version-too-big", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{binaryVersion + 1})
		assert.Equal(t, errors.New("mhash unmarshaler: version too big"), err)
	})

	t.Run("missing-next-level-byte", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{binaryVersion})
		assert.Equal(t, errors.New("mhash unmarshaler: missing next level byte"), err)
	})

	t.Run("missing-next-level-byte", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{binaryVersion})
		assert.Equal(t, errors.New("mhash unmarshaler: missing next level byte"), err)
	})

	t.Run("missing-next-level-prefix", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{binaryVersion, 2, 1, 2, 3, 4, 5, 6, 7})
		assert.Equal(t, errors.New("mhash unmarshaler: missing next level prefix"), err)
	})

	t.Run("invalid-item-len", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{
			binaryVersion, 2,
			1, 2, 3, 4, 5, 6, 7, 8,
			0xfa,
		})
		assert.Equal(t, errors.New("mhash unmarshaler: invalid item len"), err)
	})

	t.Run("invalid-data-len", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{
			binaryVersion, 2,
			1, 2, 3, 4, 5, 6, 7, 8,
			0x01,
			0xf1,
		})
		assert.Equal(t, errors.New("mhash unmarshaler: invalid data len"), err)
	})

	t.Run("invalid-data-bytes-length", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{
			binaryVersion, 2,
			1, 2, 3, 4, 5, 6, 7, 8,
			0x01,
			0x02, 0x88,
		})
		assert.Equal(t, errors.New("mhash unmarshaler: invalid data bytes length"), err)
	})

	t.Run("missing-bitset-data", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{
			binaryVersion, 2,
			1, 2, 3, 4, 5, 6, 7, 8,
			0x01,
			0x02, '{', '}',

			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,

			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33,
		})
		assert.Equal(t, errors.New("mhash unmarshaler: missing bitset data"), err)
	})

	t.Run("success", func(t *testing.T) {
		_, err := bucketUnmarshaler([]byte{
			binaryVersion, 2,
			1, 2, 3, 4, 5, 6, 7, 8,
			0x01,
			0x02, '{', '}',

			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,

			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,
			11, 22, 33, 44,
		})
		assert.Equal(t, nil, err)
	})
}
