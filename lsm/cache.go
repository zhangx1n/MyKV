package lsm

import "github.com/zhangx1n/MyKV/utils"

type cache struct {
	indexs utils.MyMap // key fid， value tableBuffer
	blocks utils.MyMap // key cacheID_blockOffset  value block []byte
}
type tableBuffer struct {
	t       *table
	cacheID int64
}
type blockBuffer struct {
	b []byte
}

// Close
func (c *cache) close() error {
	return nil
}

// NewCache
func newCache(opt *Options) *cache {
	return &cache{}
}
