package database

import (
	"context"
	"fmt"
	"time"

	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/lib/pool"
)

// DBExecutor 是数据库执行器，负责具体的命令处理
// 此处单协程处理，保证数据执行顺序性
type DBExecutor struct {
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan *def.Command

	cmdHandlers map[def.CmdType]func(*def.Command) def.Reply // 指令名称到处理函数映射
	dataStore   def.DataStore                                // 数据引擎层结构

	gcTicker *time.Ticker // 垃圾回收定时器
}

// NewDBExecutor 初始化
func NewDBExecutor(dataStore def.DataStore) def.Executor {
	ctx, cancel := context.WithCancel(context.Background())
	e := DBExecutor{
		dataStore: dataStore,
		ch:        make(chan *def.Command),
		ctx:       ctx,
		cancel:    cancel,
		gcTicker:  time.NewTicker(time.Minute),
	}
	e.cmdHandlers = map[def.CmdType]func(*def.Command) def.Reply{
		def.CmdTypeExpire:   e.dataStore.Expire,
		def.CmdTypeExpireAt: e.dataStore.ExpireAt,

		// string
		def.CmdTypeGet:  e.dataStore.Get,
		def.CmdTypeSet:  e.dataStore.Set,
		def.CmdTypeMGet: e.dataStore.MGet,
		def.CmdTypeMSet: e.dataStore.MSet,

		// list
		def.CmdTypeLPush:  e.dataStore.LPush,
		def.CmdTypeLPop:   e.dataStore.LPop,
		def.CmdTypeRPush:  e.dataStore.RPush,
		def.CmdTypeRPop:   e.dataStore.RPop,
		def.CmdTypeLRange: e.dataStore.LRange,

		// set
		def.CmdTypeSAdd:      e.dataStore.SAdd,
		def.CmdTypeSIsMember: e.dataStore.SIsMember,
		def.CmdTypeSRem:      e.dataStore.SRem,

		// hash
		def.CmdTypeHSet: e.dataStore.HSet,
		def.CmdTypeHGet: e.dataStore.HGet,
		def.CmdTypeHDel: e.dataStore.HDel,

		// sorted set
		def.CmdTypeZAdd:          e.dataStore.ZAdd,
		def.CmdTypeZRangeByScore: e.dataStore.ZRangeByScore,
		def.CmdTypeZRem:          e.dataStore.ZRem,
	}

	pool.Submit(e.run)
	return &e
}

// Entrance 指令输入入口
func (e *DBExecutor) Entrance() chan<- *def.Command {
	return e.ch
}

// ValidCommand 判断指令是否有效
func (e *DBExecutor) ValidCommand(cmd def.CmdType) bool {
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
				cmd.Receiver <- def.NewErrReply(fmt.Sprintf("unknown command '%s'", cmd.Cmd))
				continue
			}

			// 懒加载机制实现过期 key 删除
			e.dataStore.ExpirePreprocess(string(cmd.Args[0]))
			cmd.Receiver <- cmdFunc(cmd)
		}
	}
}
