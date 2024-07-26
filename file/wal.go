package file

import (
	"github.com/zhangx1n/xkv/utils"
	"github.com/zhangx1n/xkv/utils/codec"
	"os"
	"sync"
)

type WalFile struct {
	lock *sync.RWMutex
	f    *MmapFile
}

// WalFile
func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return nil
}
func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &WalFile{f: omf, lock: &sync.RWMutex{}}
}

func (wf *WalFile) Write(entry *codec.Entry) error {
	// 落预写日志简单的同步写即可
	// 序列化为磁盘结构
	walData := codec.WalCodec(entry)
	wf.lock.Lock()
	fileData, _, err := wf.f.AllocateSlice(len(walData), 0) // TODO 这里要维护offset才行
	utils.Panic(err)
	copy(fileData, walData)
	wf.lock.Unlock()
	return nil
}
