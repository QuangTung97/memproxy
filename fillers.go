package memproxy

import (
	"context"
	"fmt"
)

type multiGetFillerFactory struct {
	getFn MultiGetFunc
}

type multiGetFiller struct {
	sess Session
}

// MultiGetResponse ...
type MultiGetResponse struct {
	Outputs []MultiGetOutput
}

// MultiGetOutput ...
type MultiGetOutput struct {
	Key  any
	Data []byte
}

// MultiGetFunc ...
type MultiGetFunc func(ctx context.Context, params any, keys []any) (MultiGetResponse, error)

// NewMultiGetFillerFactory ...
func NewMultiGetFillerFactory(getFn MultiGetFunc) FillerFactory {
	return &multiGetFillerFactory{
		getFn: getFn,
	}
}

// New ...
func (*multiGetFillerFactory) New(sess Session, _ any) Filler {
	return &multiGetFiller{
		sess: sess,
	}
}

// Fill ...
func (*multiGetFiller) Fill(
	ctx context.Context, params any,
	completeFn func(resp FillResponse, err error),
) {
	fmt.Println(ctx, params, &completeFn)
}

// NewMultiGetParams ...
func NewMultiGetParams(key any) {
	fmt.Println(key)
}
