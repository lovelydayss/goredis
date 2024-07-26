package datastore

import (
	"context"
	"strconv"
	"time"

	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/lib"
)

// GC 执行过期键值对回收
// 利用 zset 的范围查询实现
func (k *KVStore) GC() {
	// 找出当前所有已过期的 key，批量回收
	nowUnix := lib.TimeNow().Unix()
	for _, expiredKey := range k.expireTimeWheel.Range(0, nowUnix) {
		k.expireProcess(expiredKey)
	}
}

// ExpirePreprocess 预处理过期键
func (k *KVStore) ExpirePreprocess(key string) {
	expiredAt, ok := k.expiredAt[key]
	if !ok {
		return
	}

	if expiredAt.After(lib.TimeNow()) {
		return
	}

	k.expireProcess(key)
}

// expireProcess 执行过期键值对回收
func (k *KVStore) expireProcess(key string) {
	delete(k.expiredAt, key)
	delete(k.data, key)
	k.expireTimeWheel.Rem(key)
}

// Expire 设置 key 的过期时间间隔
func (k *KVStore) Expire(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return def.NewSyntaxErrReply()
	}
	if ttl <= 0 {
		return def.NewErrReply("ERR invalid expire time")
	}

	expireAt := lib.TimeNow().Add(time.Duration(ttl) * time.Second)
	_cmd := [][]byte{[]byte(def.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
	return k.expireAt(cmd.Ctx, _cmd, key, expireAt)
}

// ExpireAt 设置 key 的绝对过期时间
func (k *KVStore) ExpireAt(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	expiredAt, err := lib.ParseTimeSecondFormat(string((args[1])))
	if err != nil {
		return def.NewSyntaxErrReply()
	}
	if expiredAt.Before(lib.TimeNow()) {
		return def.NewErrReply("ERR invalid expire time")
	}

	return k.expireAt(cmd.Ctx, cmd.GetCmd(), key, expiredAt)
}

// expireAt 实际设置执行
func (k *KVStore) expireAt(ctx context.Context, cmd [][]byte, key string, expireAt time.Time) def.Reply {
	k.expire(key, expireAt)
	k.persister.PersistCmd(ctx, cmd) // 持久化
	return def.NewOKReply()
}

// expire 实际设置执行
func (k *KVStore) expire(key string, expiredAt time.Time) {
	if _, ok := k.data[key]; !ok {
		return
	}
	k.expiredAt[key] = expiredAt
	k.expireTimeWheel.Add(expiredAt.Unix(), key)
}
