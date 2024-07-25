package database

import (
	"context"
	"fmt"
	"sync"

	"github.com/lovelydayss/goredis/handler"
)

// DBTrigger 触发器，对解析得到 redis 命令进行封装后分发
type DBTrigger struct {
	once     sync.Once
	executor Executor // 下层执行器
}

// NewDBTrigger 初始化
func NewDBTrigger(executor Executor) handler.DB {
	return &DBTrigger{executor: executor}
}

// Do 执行实际指令转换
func (d *DBTrigger) Do(ctx context.Context, cmdLine [][]byte) handler.Reply {
	if len(cmdLine) < 2 {
		return handler.NewErrReply(fmt.Sprintf("invalid cmd line: %v", cmdLine))
	}

	// 获取格式化指令类型名称
	cmdType := CmdType(cmdLine[0])
	if !d.executor.ValidCommand(cmdType) {
		return handler.NewErrReply(fmt.Sprintf("unknown cmd '%s'", cmdLine[0]))
	}

	// 初始化 cmd，并投递给 executor
	cmd := Command{
		Ctx:      ctx,
		Cmd:      cmdType,
		Args:     cmdLine[1:],
		Receiver: make(chan handler.Reply),
	}

	// 投递给到 executor
	d.executor.Entrance() <- &cmd

	// 监听 chan，直到接收到返回的 reply
	return <-cmd.Receiver
}

// Close 关闭触发器
func (d *DBTrigger) Close() {
	d.once.Do(d.executor.Close)
}
