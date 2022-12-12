package memproxy

import "errors"

// ErrInvalidLeaseGetResponse ...
var ErrInvalidLeaseGetResponse = errors.New("invalid lease get response")

// ErrExceededRejectRetryLimit ...
var ErrExceededRejectRetryLimit = errors.New("exceeded lease rejected retry limit")
