package xkv

import "github.com/zhangx1n/xkv/utils"

// Options 总的配置文件
type Options struct {
	ValueThreshold int64
	WorkDir        string
	MemTableSize   int64
	SSTableMaxSz   int64
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:      "./work_test",
		MemTableSize: 1024,
		SSTableMaxSz: 1 << 30,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
