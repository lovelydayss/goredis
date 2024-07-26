package persist

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lovelydayss/goredis/config"
	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/lib/pool"
)

type appendSyncStrategy string

func (a appendSyncStrategy) string() string {
	return string(a)
}

const (
	alwaysAppendSyncStrategy   appendSyncStrategy = "always"
	everysecAppendSyncStrategy appendSyncStrategy = "everysec"
	noAppendSyncStrategy       appendSyncStrategy = "no"
)

type aofPersister struct {
	ctx    context.Context
	cancel context.CancelFunc

	buffer                 chan [][]byte
	aofFile                *os.File
	aofFileName            string
	appendFsync            appendSyncStrategy
	autoAofRewriteAfterCmd int64
	aofCounter             atomic.Int64

	mu   sync.Mutex
	once sync.Once
}

func newAofPersister() (def.Persister, error) {

	AOFconf := config.Config.AOF

	aofFile, err := os.OpenFile(AOFconf.FileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := aofPersister{
		ctx:         ctx,
		cancel:      cancel,
		buffer:      make(chan [][]byte, 1<<10),
		aofFile:     aofFile,
		aofFileName: AOFconf.FileName,
	}

	if autoAofRewriteAfterCmd := AOFconf.RewriteInterval; autoAofRewriteAfterCmd > 1 {
		a.autoAofRewriteAfterCmd = int64(autoAofRewriteAfterCmd)
	}

	switch AOFconf.AppendFsync {
	case alwaysAppendSyncStrategy.string():
		a.appendFsync = alwaysAppendSyncStrategy
	case everysecAppendSyncStrategy.string():
		a.appendFsync = everysecAppendSyncStrategy
	default:
		a.appendFsync = noAppendSyncStrategy // 默认策略
	}

	pool.Submit(a.run)
	return &a, nil
}

func (a *aofPersister) Reloader() (io.ReadCloser, error) {
	file, err := os.Open(a.aofFileName)
	if err != nil {
		return nil, err
	}
	_, _ = file.Seek(0, io.SeekStart)
	return file, nil
}

func (a *aofPersister) PersistCmd(ctx context.Context, cmd [][]byte) {
	if def.IsLoadingPattern(ctx) {
		return
	}
	a.buffer <- cmd
}

func (a *aofPersister) Close() {
	a.once.Do(func() {
		a.cancel()
		_ = a.aofFile.Close()
	})
}

func (a *aofPersister) run() {
	if a.appendFsync == everysecAppendSyncStrategy {
		pool.Submit(a.fsyncEverySecond)
	}

	for {
		select {
		case <-a.ctx.Done():
			// log
			return
		case cmd := <-a.buffer:
			a.writeAof(cmd)
			a.aofTick()
		}
	}
}

// 记录执行的 aof 指令次数
func (a *aofPersister) aofTick() {
	if a.autoAofRewriteAfterCmd <= 1 {
		return
	}

	if ticked := a.aofCounter.Add(1); ticked < int64(a.autoAofRewriteAfterCmd) {
		return
	}

	// 达到重写次数，扣减计数器，进行重写
	_ = a.aofCounter.Add(-a.autoAofRewriteAfterCmd)
	pool.Submit(func() {
		if err := a.rewriteAOF(); err != nil {
			// log
		}
	})
}

func (a *aofPersister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-a.ctx.Done():
			// log
			return
		case <-ticker.C:
			if err := a.fsync(); err != nil {
				// log
			}
		}
	}
}

func (a *aofPersister) writeAof(cmd [][]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()

	persistCmd := def.NewMultiBulkReply(cmd)
	if _, err := a.aofFile.Write(persistCmd.ToBytes()); err != nil {
		// log
		return
	}

	if a.appendFsync != alwaysAppendSyncStrategy {
		return
	}

	if err := a.fsyncLocked(); err != nil {
		// log
	}
}

func (a *aofPersister) fsync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.fsyncLocked()
}

func (a *aofPersister) fsyncLocked() error {
	return a.aofFile.Sync()
}
