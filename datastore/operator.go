package datastore

import (
	mhash "github.com/lovelydayss/goredis/datastruct/hash"
	mlist "github.com/lovelydayss/goredis/datastruct/list"
	mset "github.com/lovelydayss/goredis/datastruct/set"
	msortedset "github.com/lovelydayss/goredis/datastruct/sorted_set"
	mstring "github.com/lovelydayss/goredis/datastruct/string"
	def "github.com/lovelydayss/goredis/interface"
)

// K-V 存储对应操作

func (k *KVStore) getAsString(key string) (mstring.String, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	str, ok := v.(mstring.String)
	if !ok {
		return nil, def.NewWrongTypeErrReply()
	}

	return str, nil
}

func (k *KVStore) put(key, value string, insertStrategy bool) int64 {
	if _, ok := k.data[key]; ok && insertStrategy {
		return 0
	}

	k.data[key] = mstring.NewString(key, value)
	return 1
}

func (k *KVStore) getAsList(key string) (mlist.List, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	list, ok := v.(mlist.List)
	if !ok {
		return nil, def.NewWrongTypeErrReply()
	}

	return list, nil
}

func (k *KVStore) putAsList(key string, list mlist.List) {
	k.data[key] = list
}

func (k *KVStore) getAsHashMap(key string) (mhash.HashMap, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	hmap, ok := v.(mhash.HashMap)
	if !ok {
		return nil, def.NewWrongTypeErrReply()
	}

	return hmap, nil
}

func (k *KVStore) putAsHashMap(key string, hmap mhash.HashMap) {
	k.data[key] = hmap
}

func (k *KVStore) getAsSet(key string) (mset.Set, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	set, ok := v.(mset.Set)
	if !ok {
		return nil, def.NewWrongTypeErrReply()
	}

	return set, nil
}

func (k *KVStore) putAsSet(key string, set mset.Set) {
	k.data[key] = set
}

func (k *KVStore) getAsSortedSet(key string) (msortedset.SortedSet, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	zset, ok := v.(msortedset.SortedSet)
	if !ok {
		return nil, def.NewWrongTypeErrReply()
	}

	return zset, nil
}

func (k *KVStore) putAsSortedSet(key string, zset msortedset.SortedSet) {
	k.data[key] = zset
}
