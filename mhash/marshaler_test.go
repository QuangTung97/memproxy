package mhash

import (
	"encoding/json"
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
