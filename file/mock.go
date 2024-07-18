package file

import "os"

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
}

// OpenMockFile
func OpenMockFile(opt *Options) *MockFile {
	m := &MockFile{}
	m.f, _ = os.Open(opt.Name)
	return m
}
