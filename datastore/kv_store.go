package datastore

import (
	"strconv"
	"strings"
	"time"

	mhash "github.com/lovelydayss/goredis/datastruct/hash"
	mlist "github.com/lovelydayss/goredis/datastruct/list"
	mset "github.com/lovelydayss/goredis/datastruct/set"
	msortedset "github.com/lovelydayss/goredis/datastruct/sorted_set"
	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/lib"
)

// KVStore 键值存储结构
type KVStore struct {

	// 接口 + 反射实现不同类型数据存储
	data map[string]interface{}

	// 过期时间
	expiredAt       map[string]time.Time
	expireTimeWheel msortedset.SortedSet

	// 持久化接口
	persister def.Persister
}

// NewKVStore 初始化 KVStore
func NewKVStore(persister def.Persister) def.DataStore {
	return &KVStore{
		data:            make(map[string]interface{}),
		expiredAt:       make(map[string]time.Time),
		expireTimeWheel: msortedset.NewSkiplist("expireTimeWheel"),
		persister:       persister,
	}
}

// ForEach 遍历 KVStore
func (k *KVStore) ForEach(f func(key string, adapter def.CmdAdapter, expireAt *time.Time)) {
	for key, data := range k.data {
		expiredAt, ok := k.expiredAt[key]
		if ok && expiredAt.Before(lib.TimeNow()) {
			continue
		}
		_adapter, _ := data.(def.CmdAdapter)
		if ok {
			f(key, _adapter, &expiredAt)
		} else {
			f(key, _adapter, nil)
		}
	}
}

// Get String 类型 Get 实现
func (k *KVStore) Get(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	v, err := k.getAsString(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}
	if v == nil {
		return def.NewNillReply()
	}
	return def.NewBulkReply(v.Bytes())
}

func (k *KVStore) MGet(cmd *def.Command) def.Reply {
	args := cmd.Args
	res := make([][]byte, 0, len(args))
	for _, arg := range args {
		v, err := k.getAsString(string(arg))
		if err != nil {
			return def.NewErrReply(err.Error())
		}
		if v == nil {
			res = append(res, []byte("(nil)"))
			continue
		}
		res = append(res, v.Bytes())
	}

	return def.NewMultiBulkReply(res)
}

func (k *KVStore) Set(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	value := string(args[1])

	// 支持 NX EX
	var (
		insertStrategy bool
		ttlStrategy    bool
		ttlSeconds     int64
		ttlIndex       = -1
	)

	for i := 2; i < len(args); i++ {
		flag := strings.ToLower(string(args[i]))
		switch flag {
		case "nx":
			insertStrategy = true
		case "ex":
			// 重复的 ex 指令
			if ttlStrategy {
				return def.NewSyntaxErrReply()
			}
			if i == len(args)-1 {
				return def.NewSyntaxErrReply()
			}
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return def.NewSyntaxErrReply()
			}
			if ttl <= 0 {
				return def.NewErrReply("ERR invalid expire time")
			}

			ttlStrategy = true
			ttlSeconds = ttl
			ttlIndex = i
			i++
		default:
			return def.NewSyntaxErrReply()
		}
	}

	// 将 args 剔除 ex 部分，进行持久化
	if ttlIndex != -1 {
		args = append(args[:ttlIndex], args[ttlIndex+2:]...)
	}

	// 设置
	affected := k.put(key, value, insertStrategy)
	if affected > 0 && ttlStrategy {
		expireAt := lib.TimeNow().Add(time.Duration(ttlSeconds) * time.Second)
		_cmd := [][]byte{[]byte(def.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
		_ = k.expireAt(cmd.Ctx, _cmd, key, expireAt) // 其中会完成 ex 信息的持久化
	}

	// 过期时间处理
	if affected > 0 {
		k.persister.PersistCmd(cmd.Ctx, append([][]byte{[]byte(def.CmdTypeSet)}, args...))
		return def.NewIntReply(affected)
	}

	return def.NewNillReply()
}

func (k *KVStore) MSet(cmd *def.Command) def.Reply {
	args := cmd.Args
	if len(args)&1 == 1 {
		return def.NewSyntaxErrReply()
	}

	for i := 0; i < len(args); i += 2 {
		_ = k.put(string(args[i]), string(args[i+1]), false)
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd())
	return def.NewIntReply(int64(len(args) >> 1))
}

// list
func (k *KVStore) LPush(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if list == nil {
		list = mlist.NewListEntity(key)
		k.putAsList(key, list)
	}

	for i := 1; i < len(args); i++ {
		list.LPush(args[i])
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd())
	return def.NewIntReply(list.Len())
}

func (k *KVStore) LPop(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	var cnt int64
	if len(args) > 1 {
		rawCnt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return def.NewSyntaxErrReply()
		}
		if rawCnt < 1 {
			return def.NewSyntaxErrReply()
		}
		cnt = rawCnt
	}

	list, err := k.getAsList(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if list == nil {
		return def.NewNillReply()
	}

	if cnt == 0 {
		cnt = 1
	}

	poped := list.LPop(cnt)
	if poped == nil {
		return def.NewNillReply()
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化

	if len(poped) == 1 {
		return def.NewBulkReply(poped[0])
	}

	return def.NewMultiBulkReply(poped)
}

func (k *KVStore) RPush(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if list == nil {
		list = mlist.NewListEntity(key, args[1:]...)
		k.putAsList(key, list)
		return def.NewIntReply(list.Len())
	}

	for i := 1; i < len(args); i++ {
		list.RPush(args[i])
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	return def.NewIntReply(list.Len())
}

func (k *KVStore) RPop(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	var cnt int64
	if len(args) > 1 {
		rawCnt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return def.NewSyntaxErrReply()
		}
		if rawCnt < 1 {
			return def.NewSyntaxErrReply()
		}
		cnt = rawCnt
	}

	list, err := k.getAsList(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if list == nil {
		return def.NewNillReply()
	}

	if cnt == 0 {
		cnt = 1
	}

	poped := list.RPop(cnt)
	if poped == nil {
		return def.NewNillReply()
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	if len(poped) == 1 {
		return def.NewBulkReply(poped[0])
	}

	return def.NewMultiBulkReply(poped)
}

func (k *KVStore) LRange(cmd *def.Command) def.Reply {
	args := cmd.Args
	if len(args) != 3 {
		return def.NewSyntaxErrReply()
	}

	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return def.NewSyntaxErrReply()
	}

	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return def.NewSyntaxErrReply()
	}

	list, err := k.getAsList(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if list == nil {
		return def.NewNillReply()
	}

	if got := list.Range(start, stop); got != nil {
		return def.NewMultiBulkReply(got)
	}

	return def.NewNillReply()
}

// set
func (k *KVStore) SAdd(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if set == nil {
		set = mset.NewSetEntity(key)
		k.putAsSet(key, set)
	}

	var added int64
	for _, arg := range args[1:] {
		added += set.Add(string(arg))
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	return def.NewIntReply(added)
}

func (k *KVStore) SIsMember(cmd *def.Command) def.Reply {
	args := cmd.Args
	if len(args) != 2 {
		return def.NewSyntaxErrReply()
	}

	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if set == nil {
		return def.NewIntReply(0)
	}

	return def.NewIntReply(set.Exist(string(args[1])))
}

func (k *KVStore) SRem(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if set == nil {
		return def.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args[1:] {
		remed += set.Rem(string(arg))
	}

	if remed > 0 {
		k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	}
	return def.NewIntReply(remed)
}

// hash
func (k *KVStore) HSet(cmd *def.Command) def.Reply {
	args := cmd.Args
	if len(args)&1 != 1 {
		return def.NewSyntaxErrReply()
	}

	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if hmap == nil {
		hmap = mhash.NewHashMapEntity(key)
		k.putAsHashMap(key, hmap)
	}

	for i := 0; i < len(args)-1; i += 2 {
		hkey := string(args[i+1])
		hvalue := args[i+2]
		hmap.Put(hkey, hvalue)
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	return def.NewIntReply(int64((len(args) - 1) >> 1))
}

func (k *KVStore) HGet(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if hmap == nil {
		return def.NewNillReply()
	}

	if v := hmap.Get(string(args[1])); v != nil {
		return def.NewBulkReply(v)
	}

	return def.NewNillReply()
}

func (k *KVStore) HDel(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if hmap == nil {
		return def.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args[1:] {
		remed += hmap.Del(string(arg))
	}

	if remed > 0 {
		k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	}
	return def.NewIntReply(remed)
}

// sorted set
func (k *KVStore) ZAdd(cmd *def.Command) def.Reply {
	args := cmd.Args
	if len(args)&1 != 1 {
		return def.NewSyntaxErrReply()
	}

	key := string(args[0])
	var (
		scores  = make([]int64, 0, (len(args)-1)>>1)
		members = make([]string, 0, (len(args)-1)>>1)
	)

	for i := 0; i < len(args)-1; i += 2 {
		score, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return def.NewSyntaxErrReply()
		}

		scores = append(scores, score)
		members = append(members, string(args[i+2]))
	}

	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if zset == nil {
		zset = msortedset.NewSkiplist(key)
		k.putAsSortedSet(key, zset)
	}

	for i := 0; i < len(scores); i++ {
		zset.Add(scores[i], members[i])
	}

	k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	return def.NewIntReply(int64(len(scores)))
}

func (k *KVStore) ZRangeByScore(cmd *def.Command) def.Reply {
	args := cmd.Args
	if len(args) < 3 {
		return def.NewSyntaxErrReply()
	}

	key := string(args[0])
	score1, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return def.NewSyntaxErrReply()
	}
	score2, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return def.NewSyntaxErrReply()
	}

	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if zset == nil {
		return def.NewNillReply()
	}

	rawRes := zset.Range(score1, score2)
	if len(rawRes) == 0 {
		return def.NewNillReply()
	}

	res := make([][]byte, 0, len(rawRes))
	for _, item := range rawRes {
		res = append(res, []byte(item))
	}

	return def.NewMultiBulkReply(res)
}

func (k *KVStore) ZRem(cmd *def.Command) def.Reply {
	args := cmd.Args
	key := string(args[0])
	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}

	if zset == nil {
		return def.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args {
		remed += zset.Rem(string(arg))
	}

	if remed > 0 {
		k.persister.PersistCmd(cmd.Ctx, cmd.GetCmd()) // 持久化
	}
	return def.NewIntReply(remed)
}

func (k *KVStore) SetBit(cmd *def.Command) def.Reply {

	return nil
}

func (k *KVStore) GetBit(cmd *def.Command) def.Reply {

	args := cmd.Args
	key := string(args[0])
	if len(args) != 2 {
		return def.NewSyntaxErrReply()
	}

	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return def.NewSyntaxErrReply()
	}

	v, err := k.getAsBitmap(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}
	if v == nil {
		return def.NewNillReply()
	}

	return def.NewBulkReply(v.GetBit(offset))
}

func (k *KVStore) BitCount(cmd *def.Command) def.Reply {

	args := cmd.Args
	key := string(args[0])
	if len(args) != 1 {
		return def.NewSyntaxErrReply()
	}

	v, err := k.getAsBitmap(key)
	if err != nil {
		return def.NewErrReply(err.Error())
	}
	if v == nil {
		return def.NewNillReply()
	}

	return def.NewBulkReply(v.Count())
}
