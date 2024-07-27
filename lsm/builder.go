package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/zhangx1n/xkv/file"
	"github.com/zhangx1n/xkv/iterator"
	"github.com/zhangx1n/xkv/pb"
	"github.com/zhangx1n/xkv/utils"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
	"sort"
	"unsafe"
)

// tableBuilder 负责构建SSTable
type tableBuilder struct {
	curBlock   *block   // 当前正在构建的block
	opt        *Options // SSTable的配置选项
	blockList  []*block // 已完成的block列表
	keyCount   uint32   // 键的总数
	keyHashes  []uint32 // 用于构建布隆过滤器的键哈希
	maxVersion uint64   // 最大版本号
	baseKey    []byte   // 当前block的基准键
}

// buildData 包含构建SSTable所需的所有数据
type buildData struct {
	blockList []*block // block列表
	index     []byte   // 索引数据
	checksum  []byte   // 校验和
	size      int      // 总大小
}

// block 表示SSTable中的一个数据块
type block struct {
	offset            int      // block在文件中的偏移量
	checksum          []byte   // block的校验和
	entriesIndexStart int      // 条目索引的起始位置
	chkLen            int      // 校验和长度
	data              []byte   // block的原始数据
	baseKey           []byte   // block的基准键
	entryOffsets      []uint32 // block中各条目的偏移量
	end               int      // block的结束位置
}

// header 用于压缩存储键
type header struct {
	overlap uint16 // 与base key的重叠长度
	diff    uint16 // 与base key的差异长度
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decode decodes the header.
func (h *header) decode(buf []byte) {
	// Copy over data from buf into h. Using *h=unsafe.pointer(...) leads to
	// pointer alignment issues. See https://github.com/dgraph-io/badger/issues/1096
	// and comment https://github.com/dgraph-io/badger/pull/1097#pullrequestreview-307361714
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (tb *tableBuilder) add(e *utils.Entry) {
	key := e.Key
	// 检查是否需要分配一个新的 block
	if tb.tryFinishBlock(e) {
		tb.finishBlock()
		// Create a new block and start writing.
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize), // TODO 加密block后块的大小会增加，需要预留一些填充位置
		}
	}
	// 更新keyHashes和maxVersion
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))

	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	// 计算与baseKey的差异
	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		// 如果是block的第一个键，设置为baseKey
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// 记录当前条目的偏移量
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	// 写入header、diffKey和value
	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(e.EncodedSize()))
	e.EncodeEntry(dst)
}

func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt: opt,
	}
}

// tryFinishBlock 检查是否需要结束当前block
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	// If there is no entry till now, we will return false.
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	// Integer overflow check for statements below.
	// 检查整数溢出
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	// We should include current entry also in size, that's why +1 to len(b.entryOffsets).
	// 估计添加新条目后block的大小
	entriesOffsetsSize := uint32((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	estimatedSize := uint32(tb.curBlock.end) + uint32(6 /*header size for entry*/) +
		uint32(len(e.Key)) + uint32(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(estimatedSize) < math.MaxUint32), errors.New("Integer overflow"))

	return estimatedSize > uint32(tb.opt.BlockSize)
}

/*
Structure of Block.
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry1            | Entry2              | Entry3             | Entry4       | Entry5           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry6            | ...                 | ...                | ...          | EntryN           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Block Meta(contains list of offsets used| Block Meta Size    | Block        | Checksum Size    |
| to perform binary search in the block)  | (4 Bytes)          | Checksum     | (4 Bytes)        |
+-----------------------------------------+--------------------+--------------+------------------+
*/
// In case the data is encrypted, the "IV" is added to the end of the block.
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length.
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))

	tb.blockList = append(tb.blockList, tb.curBlock)

	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	return
}

// append 向curBlock.data追加数据
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

// allocate 在curBlock.data中分配空间
func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate.
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

// keyDiff 计算newKey与baseKey的差异部分
func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

// flush 将构建好的SSTable写入存储
func (tb *tableBuilder) flush(sst *file.SSTable) error {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := sst.Bytes(0, bd.size)
	if err != nil {
		return err
	}
	copy(dst, buf)
	return nil
}

// Copy 将buildData复制到目标缓冲区
func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// done 完成SSTable的构建
func (tb *tableBuilder) done() buildData {
	tb.finishBlock() // This will never start a new block.
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}

	// 布隆过滤器构建
	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}
	// TODO 构建 sst的索引
	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

// buildIndex 构建SSTable的索引
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := proto.Marshal(tableIndex)

	utils.Panic(err)
	return data, dataSize
}

// writeBlockOffsets writes all the blockOffets in b.offsets and returns the
// offsets for the newly written items.
func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

// writeBlockOffset writes the given key,offset,len triple to the indexBuilder.
// It returns the offset of the newly written blockoffset.
func (b *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	// Write the key to the buffer.
	offset := &pb.BlockOffset{}
	// Build the blockOffset.
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte
	idx          int // Idx of the entry inside a block
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int
	// prevOverlap stores the overlap of the previous key with the base key.
	// This avoids unnecessary copy of base key when the overlap is same for multiple keys.
	prevOverlap uint16

	it iterator.Item
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We don't need it anymore.
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

// seek brings us to the first block element that is >= input key.
func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0 // This tells from which index we should start binary search.

	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// If idx is less than start index then just return false.
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

// setIdx sets the iterator to the entry at index i and set it's key and value.
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	// Set base key.
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	// idx points to the last entry in the block.
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		// idx point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				itr.tableID, itr.blockID, itr.idx, len(itr.data), startOffset, endOffset,
				len(itr.entryOffsets), itr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	// Header contains the length of key overlap and difference compared to the base key. If the key
	// before this one had the same or better key overlap, we can avoid copying that part into
	// itr.key. But, if the overlap was lesser, we could copy over just that portion.
	if h.overlap > itr.prevOverlap {
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}

	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...)
	e := utils.NewEntry(itr.key, nil)
	e.DecodeEntry(entryData[valueOff:])
	itr.it = &Item{e: e}
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) Valid() bool {
	return itr.it == nil
}
func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}
func (itr *blockIterator) Item() iterator.Item {
	return itr.it
}
func (itr *blockIterator) Close() error {
	return nil
}
