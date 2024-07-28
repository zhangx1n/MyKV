package lsm

import (
	"github.com/stretchr/testify/assert"
	"github.com/zhangx1n/xkv/utils"
	"testing"
)

var (
	// case
	entrys = []*utils.Entry{
		{Key: []byte("hello0_12345678"), Value: []byte("world0"), ExpiresAt: uint64(0)},
		{Key: []byte("hello1_12345678"), Value: []byte("world1"), ExpiresAt: uint64(0)},
		{Key: []byte("hello2_12345678"), Value: []byte("world2"), ExpiresAt: uint64(0)},
		{Key: []byte("hello3_12345678"), Value: []byte("world3"), ExpiresAt: uint64(0)},
	}
	// 初始化opt
	opt = &Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       283,
		MemTableSize:       224,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
	}
)

// 对level 管理器的功能测试
func TestFlushBase(t *testing.T) {
	lsm := buildCase()
	test := func() {
		// 测试 flush
		assert.Nil(t, lsm.levels.flush(lsm.memTable))
		// 基准chess
		baseTest(t, lsm)
	}
	// 运行N次测试多个sst的影响
	runTest(test, 2)
}

// TestRecovery _
func TestRecoveryBase(t *testing.T) {
	buildCase()
	test := func() {
		// 丢弃整个LSM结构模拟数据库崩溃恢复
		lsm := NewLSM(opt)
		// 测试正确性
		baseTest(t, lsm)
	}
	runTest(test, 1)
}

func buildCase() *LSM {
	// init DB Basic Test
	lsm := NewLSM(opt)
	for _, entry := range entrys {
		lsm.Set(entry)
	}
	return lsm
}

func baseTest(t *testing.T, lsm *LSM) {
	// 从levels中进行GET
	v, err := lsm.Get([]byte("hello7_12345678"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world7"), v.Value)
	t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)
}

func runTest(test func(), n int) {
	for i := 0; i < n; i++ {
		test()
	}
}
