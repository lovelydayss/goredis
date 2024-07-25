package database

import (
	"context"
	"strings"
	"time"

	"github.com/lovelydayss/goredis/handler"
)

const (

	// 设置过期时间
	CmdTypeExpire   CmdType = "expire"
	CmdTypeExpireAt CmdType = "expireat"

	// string
	CmdTypeGet  CmdType = "get"
	CmdTypeSet  CmdType = "set"
	CmdTypeMGet CmdType = "mget"
	CmdTypeMSet CmdType = "mset"

	// list
	CmdTypeLPush  CmdType = "lpush"
	CmdTypeLPop   CmdType = "lpop"
	CmdTypeRPush  CmdType = "rpush"
	CmdTypeRPop   CmdType = "rpop"
	CmdTypeLRange CmdType = "lrange"

	// hash
	CmdTypeHSet CmdType = "hset"
	CmdTypeHGet CmdType = "hget"
	CmdTypeHDel CmdType = "hdel"

	// set
	CmdTypeSAdd      CmdType = "sadd"
	CmdTypeSIsMember CmdType = "sismember"
	CmdTypeSRem      CmdType = "srem"

	// sorted set
	CmdTypeZAdd          CmdType = "zadd"
	CmdTypeZRangeByScore CmdType = "zrangebyscore"
	CmdTypeZRem          CmdType = "zrem"
)

// Executor 指令执行器接口
type Executor interface {
	Entrance() chan<- *Command
	ValidCommand(cmd CmdType) bool
	Close()
}

// DataStore 数据存储接口
type DataStore interface {
	ForEach(task func(key string, adapter CmdAdapter, expireAt *time.Time))

	ExpirePreprocess(key string)
	GC() // 定时回收过期 key-value

	Expire(*Command) handler.Reply
	ExpireAt(*Command) handler.Reply

	// string
	Get(*Command) handler.Reply
	MGet(*Command) handler.Reply
	Set(*Command) handler.Reply
	MSet(*Command) handler.Reply

	// list
	LPush(*Command) handler.Reply
	LPop(*Command) handler.Reply
	RPush(*Command) handler.Reply
	RPop(*Command) handler.Reply
	LRange(*Command) handler.Reply

	// set
	SAdd(*Command) handler.Reply
	SIsMember(*Command) handler.Reply
	SRem(*Command) handler.Reply

	// hash
	HSet(*Command) handler.Reply
	HGet(*Command) handler.Reply
	HDel(*Command) handler.Reply

	// sorted set
	ZAdd(*Command) handler.Reply
	ZRangeByScore(*Command) handler.Reply
	ZRem(*Command) handler.Reply
}

// CmdType 指令类型
type CmdType string

// 统一化指令名称为小写
func (c CmdType) String() string {
	return strings.ToLower(string(c))
}

// Command 指令封装类型
type Command struct {
	Ctx      context.Context
	Cmd      CmdType
	Args     [][]byte
	Receiver chan handler.Reply
}

// CmdAdapter 指令执行适配器接口
type CmdAdapter interface {
	ToCmd() [][]byte
}
