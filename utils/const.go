package utils

import (
	"hash/crc32"
	"os"
)

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold = 1024
)

// file
const (
	ManifestFilename        = "MANIFEST"
	ManifestRewriteFilename = "REWRITEMANIFEST"
	DefaultFileFlag         = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode         = 0666
)

// codec
var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable     = crc32.MakeTable(crc32.Castagnoli)
	MaxHeaderSize      int = 21
)
