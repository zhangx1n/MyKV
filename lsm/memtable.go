package lsm

import (
	"github.com/zhangx1n/MyKV/file"
	"github.com/zhangx1n/MyKV/utils"
	"github.com/zhangx1n/MyKV/utils/codec"
)

// MemTable
type memTable struct {
	wal *file.WalFile // 写到磁盘中的 wal 日志
	sl  *utils.SkipList
}

// todo: mock, need to add real logic
func NewMemtable() (*memTable, error) {
	return nil, nil
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

func (m *memTable) set(entry *codec.Entry) error {
	// 写到wal 日志中，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	if err := m.sl.Add(entry); err != nil {
		return err
	}
	return nil
}

func (m *memTable) Get(key []byte) (*codec.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	return m.sl.Search(key), nil
}

func (m *memTable) Size() int64 {
	return m.sl.Size()
}

// recovery
func recovery(opt *Options) (*memTable, []*memTable) {
	// TODO 这里需要实现获取mem list
	fileOpt := &file.Options{
		Dir:  opt.WorkDir,
		Name: "00001.mem",
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}
}
