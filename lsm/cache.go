package lsm

import "github.com/zhangx1n/xkv/utils"

type cache struct {
	indexs *utils.MyMap // key fid， value tableBuffer
	blocks *utils.MyMap // key cacheID_blockOffset  value block []byte
}

type blockBuffer struct {
	b []byte
}

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{indexs: utils.NewMap(), blocks: utils.NewMap()}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
