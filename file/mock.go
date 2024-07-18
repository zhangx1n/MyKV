package file

import (
	"fmt"
	"github.com/zhangx1n/MyKV/utils"
	"os"
)

// MockFile
type MockFile struct {
	f *os.File
}

func (m MockFile) Write(b []byte) (n int, err error) {
	return m.f.Write(b)
}

func (m MockFile) Read(b []byte) (n int, err error) {
	return m.f.Read(b)
}

func (m MockFile) Close() error {
	if err := m.f.Close(); err != nil {
		return err
	}
	return nil
}

// Options
type Options struct {
	Name string
	Dir  string
}

// OpenMockFile mock 文件
func OpenMockFile(opt *Options) *MockFile {
	var err error
	lf := &MockFile{}
	lf.f, err = os.Open(fmt.Sprintf("%s/%s", opt.Dir, opt.Name))
	utils.Panic(err)
	return lf
}
