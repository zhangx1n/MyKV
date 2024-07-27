package utils

import (
	"encoding/binary"
	"time"
)

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

// EncodedSize is the size of the ValueStruct when encoded
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// DecodeEntry _
func (e *Entry) DecodeEntry(buf []byte) {
	var sz int
	e.ExpiresAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

func (e *Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}

// EncodeEntry expects a slice of length at least v.EncodedSize().
func (e *Entry) EncodeEntry(b []byte) uint32 {
	sz := binary.PutUvarint(b[:], e.ExpiresAt)
	n := copy(b[sz:], e.Value)
	return uint32(sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
