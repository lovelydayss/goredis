package def

import (
	"context"
	"io"
)

// Persister 持久化组件接口定义
type Persister interface {
	Reloader() (io.ReadCloser, error)
	PersistCmd(ctx context.Context, cmd [][]byte)
	Close()
}

var loadingPersisterPattern int
var ctxKeyLoadingPersisterPattern = &loadingPersisterPattern

// SetLoadingPattern 设置初始化加载模式
func SetLoadingPattern(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyLoadingPersisterPattern, true)
}

// IsLoadingPattern 判断初始化加载模式
func IsLoadingPattern(ctx context.Context) bool {
	is, _ := ctx.Value(ctxKeyLoadingPersisterPattern).(bool)
	return is
}

type fakeReadWriter struct {
	io.Reader
}

// NewFakeReaderWriter 创建fake read writer
func NewFakeReaderWriter(reader io.Reader) io.ReadWriter {
	return &fakeReadWriter{
		Reader: reader,
	}
}

func (f *fakeReadWriter) Write(p []byte) (n int, err error) {
	// log ...
	return 0, nil
}
