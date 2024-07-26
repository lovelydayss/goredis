package mhash

import def "github.com/lovelydayss/goredis/interface"

// HashMap hash表结构接口
type HashMap interface {
	Put(key string, value []byte)
	Get(key string) []byte
	Del(key string) int64
	def.CmdAdapter
}

// hashMapEntity hash表实体结构
type hashMapEntity struct {
	key  string
	data map[string][]byte
}

// NewHashMapEntity 初始化hash表实体
func NewHashMapEntity(key string) HashMap {
	return &hashMapEntity{
		key:  key,
		data: make(map[string][]byte),
	}
}

// Put 添加一个值
func (h *hashMapEntity) Put(key string, value []byte) {
	h.data[key] = value
}

// Get 获取一个值
func (h *hashMapEntity) Get(key string) []byte {
	return h.data[key]
}

// Del 删除一个值
func (h *hashMapEntity) Del(key string) int64 {
	if _, ok := h.data[key]; !ok {
		return 0
	}
	delete(h.data, key)
	return 1
}

// ToCmd Redis 命令解析
func (h *hashMapEntity) ToCmd() [][]byte {
	args := make([][]byte, 0, 2+2*len(h.data))
	args = append(args, []byte(def.CmdTypeHSet), []byte(h.key))
	for k, v := range h.data {
		args = append(args, []byte(k), v)
	}
	return args
}
