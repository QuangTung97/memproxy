package mmap

import (
	"github.com/QuangTung97/memproxy/item"
)

type mapConfig struct {
	itemOptions []item.Option
	separator   string
}

func computeMapConfig(options []MapOption) mapConfig {
	conf := mapConfig{
		itemOptions: nil,
		separator:   ":",
	}
	for _, fn := range options {
		fn(&conf)
	}
	return conf
}

// MapOption ...
type MapOption func(conf *mapConfig)

// WithItemOptions ...
func WithItemOptions(options ...item.Option) MapOption {
	return func(conf *mapConfig) {
		conf.itemOptions = options
	}
}

// WithSeparator ...
func WithSeparator(sep string) MapOption {
	return func(conf *mapConfig) {
		conf.separator = sep
	}
}
