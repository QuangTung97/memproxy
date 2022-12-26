package mapcache

type invalidatorFactoryImpl struct {
}

// NewInvalidatorFactory ...
func NewInvalidatorFactory() InvalidatorFactory {
	return &invalidatorFactoryImpl{}
}

func (*invalidatorFactoryImpl) New(rootKey string, sizeLog SizeLog) Invalidator {
	return &invalidatorImpl{
		conf: mapCacheConfig{
			rootKey: rootKey,
			sizeLog: sizeLog,
		},
	}
}

type invalidatorImpl struct {
	conf mapCacheConfig
}

// DeleteKeys ...
func (i *invalidatorImpl) DeleteKeys(
	key string, _ DeleteKeyOptions,
) []string {
	keyHash := hashFunc(key)

	result := make([]string, 0, 3)
	result = append(result, i.conf.getHighCacheKey(keyHash))
	if i.conf.sizeLog.Previous > i.conf.sizeLog.Current {
		hashRange := computeHashRange(keyHash, i.conf.sizeLog.Current)
		result = append(result, i.conf.getLowCacheKey(hashRange.Begin))
		result = append(result, i.conf.getLowCacheKey(hashRange.End))
	} else {
		result = append(result, i.conf.getLowCacheKey(keyHash))
	}
	return result
}
