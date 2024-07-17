package lsm

import "github.com/zhangx1n/MyKV/file"

type table struct {
	ss      *file.SSTable
	idxData []byte
}

func openTable(opt *Options) *table {
	return &table{ss: file.OpenSStable(&file.Options{})}
}
