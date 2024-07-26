package iterator

import (
	"github.com/zhangx1n/xkv/utils/codec"
)

// Iterator 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

type Item interface {
	Entry() *codec.Entry
}

type Options struct {
	Prefix []byte // 用于前缀过滤
	IsAsc  bool   // true: 升序, false: 降序
}
