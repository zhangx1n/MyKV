package xkv

import (
	"github.com/zhangx1n/xkv/utils"
)

type DBIterator struct {
	iters []utils.Iterator
}
type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	dbIter := &DBIterator{}
	dbIter.iters = make([]utils.Iterator, 0)
	dbIter.iters = append(dbIter.iters, db.lsm.NewIterator(opt))
	return dbIter
}

func (iter *DBIterator) Next() {
	iter.iters[0].Next()
}
func (iter *DBIterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *DBIterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *DBIterator) Item() utils.Item {
	return iter.iters[0].Item()
}
func (iter *DBIterator) Close() error {
	return nil
}
func (iter *DBIterator) Seek(key []byte) {
}
