package utils

import "sync"

type MyMap struct {
	m sync.Map
}

// NewMap
func NewMap() *MyMap {
	return &MyMap{m: sync.Map{}}
}

// Get
func (c *MyMap) Get(key interface{}) (interface{}, bool) {
	return c.m.Load(key)
}

// Set
func (c *MyMap) Set(key, value interface{}) {
	c.m.Store(key, value)
}

// Range
func (c *MyMap) Range(f func(key, value interface{}) bool) {
	c.m.Range(f)
}
