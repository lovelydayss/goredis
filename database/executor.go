package database

import (
	"context"
	"fmt"
	"time"

	"github.com/lovelydayss/goredis/handler"
	"github.com/lovelydayss/goredis/lib/pool"
)

// DBExecutor 是数据库执行器，负责具体的命令处理
// 此处单协程处理，保证数据执行顺序性
type DBExecutor struct {
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan *Command

	cmdHandlers map[CmdType]func(*Command) handler.Reply // 指令名称到处理函数映射
	dataStore   DataStore                                // 数据引擎层结构

	gcTicker *time.Ticker // 垃圾回收定时器
}

// NewDBExecutor 初始化
func NewDBExecutor(dataStore DataStore) Executor {
	ctx, cancel := context.WithCancel(context.Background())
	e := DBExecutor{
		dataStore: dataStore,
		ch:        make(chan *Command),
		ctx:       ctx,
		cancel:    cancel,
		gcTicker:  time.NewTicker(time.Minute),
	}
	e.cmdHandlers = map[CmdType]func(*Command) handler.Reply{
		CmdTypeExpire:   e.dataStore.Expire,
		CmdTypeExpireAt: e.dataStore.ExpireAt,

		// string
		CmdTypeGet:  e.dataStore.Get,
		CmdTypeSet:  e.dataStore.Set,
		CmdTypeMGet: e.dataStore.MGet,
		CmdTypeMSet: e.dataStore.MSet,

		// list
		CmdTypeLPush:  e.dataStore.LPush,
		CmdTypeLPop:   e.dataStore.LPop,
		CmdTypeRPush:  e.dataStore.RPush,
		CmdTypeRPop:   e.dataStore.RPop,
		CmdTypeLRange: e.dataStore.LRange,

		// set
		CmdTypeSAdd:      e.dataStore.SAdd,
		CmdTypeSIsMember: e.dataStore.SIsMember,
		CmdTypeSRem:      e.dataStore.SRem,

		// hash
		CmdTypeHSet: e.dataStore.HSet,
		CmdTypeHGet: e.dataStore.HGet,
		CmdTypeHDel: e.dataStore.HDel,

		// sorted set
		CmdTypeZAdd:          e.dataStore.ZAdd,
		CmdTypeZRangeByScore: e.dataStore.ZRangeByScore,
		CmdTypeZRem:          e.dataStore.ZRem,
	}

	pool.Submit(e.run)
	return &e
}

// Entrance 指令输入入口
func (e *DBExecutor) Entrance() chan<- *Command {
	return e.ch
}

// ValidCommand 判断指令是否有效
func (e *DBExecutor) ValidCommand(cmd CmdType) bool {
	_, valid := e.cmdHandlers[cmd] // map 只读，不考虑并发问题
	return valid
}

// Close 关闭执行器
func (e *DBExecutor) Close() {
	e.cancel()
}

// Executor 执行器执行
func (e *DBExecutor) run() {
	for {
		select {
		case <-e.ctx.Done():
			return

		// 每隔 1 分钟批量一次过期的 key
		case <-e.gcTicker.C:
			e.dataStore.GC()

		// 指令处理
		case cmd := <-e.ch:

			// 调用对应 cmdHandlers 函数进行指令处理， 获取返回消息
			cmdFunc, ok := e.cmdHandlers[cmd.Cmd]
			if !ok {
				cmd.Receiver <- handler.NewErrReply(fmt.Sprintf("unknown command '%s'", cmd.Cmd))
				continue
			}

			// 懒加载机制实现过期 key 删除
			e.dataStore.ExpirePreprocess(string(cmd.Args[0]))
			cmd.Receiver <- cmdFunc(cmd)
		}
	}
}
