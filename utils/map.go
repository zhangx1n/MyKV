package utils

import (
	"github.com/pkg/errors"
	"github.com/zhangx1n/xkv/utils/codec"
	"reflect"
	"sync"
)

// MyMap _
type MyMap struct {
	m sync.Map
}

// NewMap _
func NewMap() *MyMap {
	return &MyMap{m: sync.Map{}}
}

// Get _
func (c *MyMap) Get(key interface{}) (interface{}, bool) {
	hashKey := c.keyToHash(key)
	return c.m.Load(hashKey)
}

// Set _
func (c *MyMap) Set(key, value interface{}) {
	hashKey := c.keyToHash(key)
	c.m.Store(hashKey, value)
}

// Range _
func (c *MyMap) Range(f func(key, value interface{}) bool) {
	c.m.Range(f)
}

func (c *MyMap) keyToHash(key interface{}) uint64 {
	if key == nil {
		return 0
	}
	switch k := key.(type) {
	case []byte:
		return codec.MemHash(k)
	case uint32:
		return uint64(k)
	case string:
		return codec.MemHashString(k)
	case uint64:
		return k
	case byte:
		return uint64(k)
	case int:
		return uint64(k)
	case int32:
		return uint64(k)

	case int64:
		return uint64(k)
	default:
		CondPanic(true, errors.Errorf("Key:[%+v] type not supported", reflect.TypeOf(k)))
	}
	return 0
}
