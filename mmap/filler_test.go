package mmap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type multiGetFillerTest struct {
	filler Filler[stockLocation, stockLocationRootKey]

	fillKeys [][]FillKey[stockLocationRootKey]

	fillFunc func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error)
}

func newMultiGetFillerTest() *multiGetFillerTest {
	f := &multiGetFillerTest{}

	f.filler = NewMultiGetFiller[stockLocation, stockLocationRootKey](
		func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error) {
			f.fillKeys = append(f.fillKeys, keys)
			return f.fillFunc(ctx, keys)
		},
		stockLocation.getRootKey,
		stockLocation.getKey,
	)

	return f
}
func TestNewMultiGetFiller(t *testing.T) {
	const sku1 = "SKU01"
	const sku2 = "SKU02"
	const sku3 = "SKU03"

	const loc1 = "LOC01"
	const loc2 = "LOC02"

	hash1 := HashRange{
		Begin: newHash(0x1000, 2),
		End:   newHash(0x1fff, 2),
	}
	hash2 := HashRange{
		Begin: newHash(0x2000, 2),
		End:   newHash(0x2fff, 2),
	}

	t.Run("single", func(t *testing.T) {
		f := newMultiGetFillerTest()

		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1.Begin,
			Quantity: 41,
		}

		f.fillFunc = func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error) {
			return []stockLocation{stock1}, nil
		}

		fn := f.filler(context.Background(), stock1.getRootKey(), hash1)

		// check resp
		resp, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []stockLocation{
			stock1,
		}, resp)

		assert.Equal(t, [][]FillKey[stockLocationRootKey]{
			{
				{RootKey: stock1.getRootKey(), Range: hash1},
			},
		}, f.fillKeys)

		// Get Again
		stock2 := stockLocation{
			Sku:      sku2,
			Location: loc2,
			Hash:     hash2.Begin,
			Quantity: 42,
		}

		f.fillFunc = func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error) {
			return []stockLocation{stock2}, nil
		}

		fn1 := f.filler(context.Background(), stock1.getRootKey(), hash1)
		fn2 := f.filler(context.Background(), stock2.getRootKey(), hash2)

		// check resp
		resp, err = fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(resp))

		resp, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, []stockLocation{
			stock2,
		}, resp)

		assert.Equal(t, [][]FillKey[stockLocationRootKey]{
			{
				{RootKey: stock1.getRootKey(), Range: hash1},
			},
			{
				{RootKey: stock1.getRootKey(), Range: hash1},
				{RootKey: stock2.getRootKey(), Range: hash2},
			},
		}, f.fillKeys)
	})

	t.Run("multiple keys", func(t *testing.T) {
		f := newMultiGetFillerTest()

		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1.Begin,
			Quantity: 41,
		}
		stock2 := stockLocation{
			Sku:      sku2,
			Location: loc2,
			Hash:     hash2.Begin,
			Quantity: 42,
		}

		f.fillFunc = func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error) {
			return []stockLocation{stock1, stock2}, nil
		}

		fn1 := f.filler(context.Background(), stock1.getRootKey(), hash1)
		fn2 := f.filler(context.Background(), stock2.getRootKey(), hash2)
		fn3 := f.filler(context.Background(), stockLocationRootKey{sku: sku3}, hash1)

		resp, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, []stockLocation{
			stock1,
		}, resp)

		resp, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, []stockLocation{
			stock2,
		}, resp)

		resp, err = fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(resp))

		assert.Equal(t, [][]FillKey[stockLocationRootKey]{
			{
				{RootKey: stock1.getRootKey(), Range: hash1},
				{RootKey: stock2.getRootKey(), Range: hash2},
				{RootKey: stockLocationRootKey{sku: sku3}, Range: hash1},
			},
		}, f.fillKeys)
	})

	t.Run("with error", func(t *testing.T) {
		f := newMultiGetFillerTest()

		f.fillFunc = func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error) {
			return nil, errors.New("fill error")
		}

		fn := f.filler(context.Background(), stockLocationRootKey{sku: sku1}, hash1)

		resp, err := fn()
		assert.Equal(t, errors.New("fill error"), err)
		assert.Equal(t, 0, len(resp))

		assert.Equal(t, [][]FillKey[stockLocationRootKey]{
			{
				{RootKey: stockLocationRootKey{sku: sku1}, Range: hash1},
			},
		}, f.fillKeys)
	})

	t.Run("multiple same root key", func(t *testing.T) {
		f := newMultiGetFillerTest()

		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1.Begin + 100,
			Quantity: 41,
		}
		stock2 := stockLocation{
			Sku:      sku1,
			Location: loc2,
			Hash:     hash2.Begin + 100,
			Quantity: 42,
		}

		f.fillFunc = func(ctx context.Context, keys []FillKey[stockLocationRootKey]) ([]stockLocation, error) {
			return []stockLocation{stock2, stock1}, nil
		}

		fn1 := f.filler(context.Background(), stock1.getRootKey(), hash1)
		fn2 := f.filler(context.Background(), stock2.getRootKey(), hash2)

		resp, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, []stockLocation{
			stock1,
		}, resp)

		resp, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, []stockLocation{
			stock2,
		}, resp)

		assert.Equal(t, [][]FillKey[stockLocationRootKey]{
			{
				{RootKey: stock1.getRootKey(), Range: hash1},
				{RootKey: stock2.getRootKey(), Range: hash2},
			},
		}, f.fillKeys)
	})
}
