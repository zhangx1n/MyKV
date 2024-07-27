package utils

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
)

// LogEntry
type LogEntry func(e *Entry, vp *ValuePtr) error

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	ExpiresAt uint64
}

const maxHeaderSize int = 21

func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

func (h *WalHeader) Decode(reader *HashReader) (int, error) {
	var err error
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// WalCodec 写入wal文件的编码
// | header | key | value | crc32 |
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}

	hash := crc32.New(CastagnoliCrcTable)
	// 将数据同时写入 buf 和 hash，以便在写入数据的同时计算 CRC32 校验和。
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	// 写入 header，key, value
	Panic2(writer.Write(headerEnc[:sz]))
	Panic2(writer.Write(e.Key))
	Panic2(writer.Write(e.Value))
	// 创建一个字节数组 crcBuf，用于存储 CRC32 校验和。调用 hash.Sum32 获取当前哈希值，
	// 并将其写入 crcBuf。然后将 crcBuf 写入 buf。
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	Panic2(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int // Number of bytes read.
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.R.Read(p)
	if err != nil {
		return n, err
	}
	t.BytesRead += n
	return t.H.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *HashReader) Sum32() uint32 {
	return t.H.Sum32()
}

// IsZero _
func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}

// LogHeaderLen _
func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

// LogOffset _
func (e *Entry) LogOffset() uint32 {
	return e.Offset
}
