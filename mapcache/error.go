package mapcache

import "errors"

// ErrMissingBucketContent ...
var ErrMissingBucketContent = errors.New("missing bucket content")

// ErrInvalidBucketContentVersion ...
var ErrInvalidBucketContentVersion = errors.New("invalid bucket content version")

// ErrMissingSizeLogOrigin ...
var ErrMissingSizeLogOrigin = errors.New("missing size log origin version")

// ErrMissingLength ...
var ErrMissingLength = errors.New("missing entry length")
