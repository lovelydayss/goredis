package def

import (
	"context"
	"strings"
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
	Receiver chan Reply
}

// CmdAdapter 指令执行适配器接口
type CmdAdapter interface {
	ToCmd() [][]byte
}
