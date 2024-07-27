package lsm

import (
	"github.com/zhangx1n/xkv/utils"
)

type Iterator struct {
	it    Item
	iters []utils.Iterator
}
type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}

// 创建迭代器
func (lsm *LSM) NewIterator(opt *utils.Options) utils.Iterator {
	iter := &Iterator{}
	iter.iters = make([]utils.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, imm := range lsm.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.NewIterator(opt))
	return iter
}
func (iter *Iterator) Next() {
	iter.iters[0].Next()
}
func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *Iterator) Item() utils.Item {
	return iter.iters[0].Item()
}
func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}

// 内存表迭代器
type memIterator struct {
	innerIter utils.Iterator
}

func (m *memTable) NewIterator(opt *utils.Options) utils.Iterator {
	return &memIterator{innerIter: m.sl.NewSkipListIterator()}
}
func (iter *memIterator) Next() {
	iter.innerIter.Next()
}
func (iter *memIterator) Valid() bool {
	return iter.innerIter.Valid()
}
func (iter *memIterator) Rewind() {
	iter.innerIter.Rewind()
}
func (iter *memIterator) Item() utils.Item {
	return iter.innerIter.Item()
}
func (iter *memIterator) Close() error {
	return iter.innerIter.Close()
}
func (iter *memIterator) Seek(key []byte) {
}

// levelManager上的迭代器
type levelIterator struct {
	it    *utils.Item
	iters []*Iterator
}

func (lm *levelManager) NewIterator(options *utils.Options) utils.Iterator {
	return &levelIterator{}
}
func (iter *levelIterator) Next() {
}
func (iter *levelIterator) Valid() bool {
	return false
}
func (iter *levelIterator) Rewind() {

}
func (iter *levelIterator) Item() utils.Item {
	return &Item{}
}
func (iter *levelIterator) Close() error {
	return nil
}

func (iter *levelIterator) Seek(key []byte) {
}
