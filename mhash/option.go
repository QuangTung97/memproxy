package mhash

import "github.com/QuangTung97/memproxy/item"

type hashConfig struct {
	itemOptions []item.Option
}

// Option ...
type Option func(conf *hashConfig)

// WithItemOptions ...
func WithItemOptions(options ...item.Option) Option {
	return func(conf *hashConfig) {
		conf.itemOptions = options
	}
}

func computeConfig(options ...Option) *hashConfig {
	conf := &hashConfig{}
	for _, opt := range options {
		opt(conf)
	}
	return conf
}
