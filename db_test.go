package xkv

import (
	"github.com/zhangx1n/xkv/utils"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	// 写入
	e := utils.NewEntry([]byte("hello"), []byte("xKV")).WithTTL(1 * time.Second)
	if err := db.Set(e); err != nil {
		t.Fatal(err)
	}
	// 查询
	if entry, err := db.Get([]byte("hello")); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
	}
	// 迭代器
	iter := db.NewIterator(&utils.Options{
		Prefix: []byte("hello"),
		IsAsc:  false,
	})
	defer func() { _ = iter.Close() }()
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}
	t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
	// 删除
	if err := db.Del([]byte("hello")); err != nil {
		t.Fatal(err)
	}
}

func FuzzAPI(f *testing.F) {
	// 添加种子语料，必须和闭包函数的模糊参数一一对应
	f.Add([]byte("x"), []byte("kv"))
	clearDir()
	db := Open(opt)
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = 1 << 10
	defer func() { _ = db.Close() }()
	// 运行 fuzz 引擎，不断生成测试用例进行测试
	f.Fuzz(func(t *testing.T, key, value []byte) {
		// 写入
		e := utils.NewEntry(key, value).WithTTL(100 * time.Second)
		if err := db.Set(e); err != nil {
			if err != utils.ErrEmptyKey {
				t.Fatalf("db.Set key=%s, value=%s, expiresAt=%d, err=%+v", e.Key, e.Value, e.ExpiresAt, err)
			}
		}
		// 查询
		if entry, err := db.Get(key); err != nil {
			if err != utils.ErrEmptyKey {
				t.Fatalf("db.Get key=%s, value=%s, expiresAt=%d, err=%+v", e.Key, e.Value, e.ExpiresAt, err)
			}
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
		// 迭代器
		iter := db.NewIterator(&utils.Options{
			IsAsc: false,
		})
		defer func() { _ = iter.Close() }()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			it := iter.Item()
			t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
		}
		t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
		// 删除
		if err := db.Del(key); err != nil {
			if err != utils.ErrEmptyKey {
				t.Fatalf("db.del key=%s, value=%s, expiresAt=%d, err=%+v", e.Key, e.Value, e.ExpiresAt, err)
			}
		}
	})
}
