package utils

import (
	"strconv"
	"strings"
)

// FID 根据file name 获取其fid
func FID(name string) uint32 {
	ns := strings.Split(name, "/")
	if len(ns) == 0 {
		return 0
	}
	tableName := ns[len(ns)-1]
	j := 0
	for i := range tableName {
		if tableName[i] != '0'-0 {
			break
		}
		j++
	}
	fidStr := tableName[j:]
	if len(fidStr) == 0 {
		return 0
	}
	ss := strings.Split(fidStr, ".")[0]
	fid, err := strconv.ParseUint(ss, 10, 32)
	Panic(err)
	return uint32(fid)
}
