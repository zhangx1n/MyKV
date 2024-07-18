package file

type MyFile interface {
	Write(b []byte) (n int, err error)
	Read(b []byte) (n int, err error)
	Close() error
	Truncature(n int64) error
	ReName(name string) error
}
