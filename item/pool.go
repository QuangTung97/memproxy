package item

import (
	"sync"
)

var getStateCommonPool = sync.Pool{
	New: func() any {
		return &getStateCommon{}
	},
}

func newGetStateCommon() *getStateCommon {
	return getStateCommonPool.Get().(*getStateCommon)
}

func putGetStateCommon(s *getStateCommon) {
	*s = getStateCommon{}
	getStateCommonPool.Put(s)
}
