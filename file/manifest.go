package file

import (
	"bufio"
	"encoding/csv"
	"github.com/zhangx1n/MyKV/utils"
	"io"
)

// Manifest manifest 文件记录 db 的元数据
type Manifest struct {
	f      MyFile
	tables [][]string // l0-l7 的sst file name
}

// WalFile
func (mf *Manifest) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// Tables 获取table的list
func (mf *Manifest) Tables() [][]string {
	return mf.tables
}

// OpenManifest 打开和读取 manifest 文件，并将其中的表信息存储在 Manifest 结构体中
func OpenManifest(opt *Options) *Manifest {
	mf := &Manifest{
		f:      OpenMockFile(opt),
		tables: make([][]string, utils.MaxLevelNum),
	}
	reader := csv.NewReader(bufio.NewReader(mf.f))
	level := 0
	for {
		if level > utils.MaxLevelNum {
			break
		}
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if len(mf.tables[level]) == 0 {
			mf.tables[level] = make([]string, len(line))
		}
		for j, tableName := range line {
			mf.tables[level][j] = tableName
		}
		level++
	}
	return mf
}
