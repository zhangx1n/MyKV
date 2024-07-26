package file

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/zhangx1n/xkv/utils"
	"github.com/zhangx1n/xkv/utils/codec"
	"github.com/zhangx1n/xkv/utils/codec/pb"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"sync"
)

// SSTable 文件的内存封装
type SSTable struct {
	lock           *sync.RWMutex
	f              *MmapFile
	maxKey         []byte
	minKey         []byte
	idxTables      *pb.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint32
}

// OpenSStable 打开一个 sst文件
func OpenSStable(opt *Options) *SSTable {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
}

// Init 初始化
func (ss *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = ss.initTable(); err != nil {
		return err
	}
	// init min key
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey

	// init max key
	blockLen := len(ss.idxTables.Offsets)
	ko = ss.idxTables.Offsets[blockLen-1]
	keyBytes = ko.GetKey()
	maxKey := make([]byte, 0)
	copy(maxKey, keyBytes)
	ss.maxKey = maxKey
	return nil
}

func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)

	// Read checksum len from the last 4 bytes.
	readPos -= 4
	buf := ss.readCheckError(readPos, 4)
	checksumLen := int(codec.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// Read checksum.
	readPos -= checksumLen
	expectedChk := ss.readCheckError(readPos, checksumLen)

	// Read index size from the footer.
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(codec.BytesToU32(buf))

	// Read index.
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTables = indexTable

	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

// Indexs _
func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTables
}

// MaxKey 当前最大的key
func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

// MinKey 当前最小的key
func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

// FID 获取fid
func (ss *SSTable) FID() uint32 {
	return ss.fid
}

// HasBloomFilter _
func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

// LoadData 加载数据块
func (ss *SSTable) LoadData() (blocks [][]byte, offsets []int) {
	ss.lock.RLock()
	fileData := ss.f.Slice(0)
	ss.lock.RUnlock()
	m := make(map[string]interface{}, 0)
	json.Unmarshal(fileData, &m)
	if data, ok := m["data"]; !ok {
		panic("sst data is nil")
	} else {
		// TODO 所有的数据都放在一个 block中
		dd := data.(string)
		blocks = append(blocks, []byte(dd))
		offsets = append(offsets, 0)
	}
	return blocks, offsets
}
func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}

	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}
func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}
