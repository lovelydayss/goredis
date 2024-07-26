package def

import (
	"context"
	"time"
)

// DB 数据库层接口
type DB interface {
	Do(ctx context.Context, cmdLine [][]byte) Reply
	Close()
}

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

	Expire(*Command) Reply
	ExpireAt(*Command) Reply

	// string
	Get(*Command) Reply
	MGet(*Command) Reply
	Set(*Command) Reply
	MSet(*Command) Reply

	// list
	LPush(*Command) Reply
	LPop(*Command) Reply
	RPush(*Command) Reply
	RPop(*Command) Reply
	LRange(*Command) Reply

	// set
	SAdd(*Command) Reply
	SIsMember(*Command) Reply
	SRem(*Command) Reply

	// hash
	HSet(*Command) Reply
	HGet(*Command) Reply
	HDel(*Command) Reply

	// sorted set
	ZAdd(*Command) Reply
	ZRangeByScore(*Command) Reply
	ZRem(*Command) Reply

	// bitmap
	SetBit(*Command) Reply
	GetBit(*Command) Reply
	BitCount(*Command) Reply
}
