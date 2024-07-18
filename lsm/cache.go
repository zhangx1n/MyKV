package lsm

import "github.com/zhangx1n/MyKV/utils"

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

// TODO fid 使用字符串是不是会有性能损耗
func (c *cache) addIndex(fid string, t *table) {
	c.indexs.Set(fid, t)
}
