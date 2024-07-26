package database

import (
	"context"
	"fmt"
	"sync"

	def "github.com/lovelydayss/goredis/interface"
)

// DBTrigger 触发器，对解析得到 redis 命令进行封装后分发
// DB 类型接口实例化
type DBTrigger struct {
	once     sync.Once
	executor def.Executor // 下层执行器
}

// NewDBTrigger 初始化
func NewDBTrigger(executor def.Executor) def.DB {
	return &DBTrigger{executor: executor}
}

// Do 执行实际指令转换
func (d *DBTrigger) Do(ctx context.Context, cmdLine [][]byte) def.Reply {
	if len(cmdLine) < 2 {
		return def.NewErrReply(fmt.Sprintf("invalid cmd line: %v", cmdLine))
	}

	// 获取格式化指令类型名称
	cmdType := def.CmdType(cmdLine[0])
	if !d.executor.ValidCommand(cmdType) {
		return def.NewErrReply(fmt.Sprintf("unknown cmd '%s'", cmdLine[0]))
	}

	// 初始化 cmd，并投递给 executor
	cmd := def.Command{
		Ctx:      ctx,
		Cmd:      cmdType,
		Args:     cmdLine[1:],
		Receiver: make(chan def.Reply),
	}

	// 投递给到 executor，实现从多连接并发到单个协程依次处理请求
	d.executor.Entrance() <- &cmd

	// 监听 chan，直到接收到返回的 reply
	return <-cmd.Receiver
}

// Close 关闭触发器
func (d *DBTrigger) Close() {
	d.once.Do(d.executor.Close)
}
