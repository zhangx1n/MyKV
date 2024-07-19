package file

import (
	"encoding/json"
	"github.com/zhangx1n/MyKV/utils"
	"sync"
)

// Manifest 维护sst文件元信息的文件
type Manifest struct {
	lock   *sync.RWMutex
	opt    *Options
	f      MyFile
	tables [][]*Cell // l0-l7 的sst file name
}

// Cell 是一行Manifest的封装
type Cell struct {
	SSTName string
}

// Close
func (mf *Manifest) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// Tables 获取table的list
func (mf *Manifest) Tables() [][]*Cell {
	return mf.tables
}

// OpenManifest
func OpenManifest(opt *Options) *Manifest {
	mf := &Manifest{
		tables: make([][]*Cell, utils.MaxLevelNum),
		opt:    opt,
		lock:   &sync.RWMutex{},
	}
	mmapFile, err := OpenMmapFile(opt.FileName, opt.Flag, opt.MaxSz)
	utils.Panic(err)
	mf.f = mmapFile
	data := mf.f.Slice(0) // all in bytes
	// 如果是新创建的数据库则直接启动，不需要加载sst
	if len(data) == 0 {
		return mf
	}
	tables := make([][]string, 0)
	utils.Panic(json.Unmarshal(data, &tables))
	// TODO 如果文件损坏或者为空则根据之前的检查点来恢复较旧的manifest文件
	for i, ts := range tables {
		mf.tables[i] = make([]*Cell, 0)
		for _, name := range ts {
			mf.tables[i] = append(mf.tables[i], &Cell{SSTName: name})
		}
	}
	return mf
}

// AppendSST 存储level表到manifest的level中
func (mf *Manifest) AppendSST(levelNum int, cell *Cell) (err error) {
	mf.tables[levelNum] = append(mf.tables[levelNum], cell)
	res := make([][]string, len(mf.tables))
	for i, cells := range mf.tables {
		res[i] = make([]string, 0)
		for _, cell := range cells {
			res[i] = append(res[i], cell.SSTName)
		}
	}
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	// fmt.Println(string(data))
	// panic(data)
	// err = mf.f.Delete()
	// if err != nil {
	// 	return err
	// }
	mf.lock.Lock()
	defer mf.lock.Unlock()
	// TODO 保留旧的MANIFEST文件作为检查点，当前直接截断
	fileData, _, err := mf.f.AllocateSlice(len(data), 0)
	utils.Panic(err)
	copy(fileData, data)
	return err
}
