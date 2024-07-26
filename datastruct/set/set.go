package mset

import def "github.com/lovelydayss/goredis/interface"

// Set 集合数据结构接口
type Set interface {
	Add(value string) int64
	Exist(value string) int64
	Rem(value string) int64
	def.CmdAdapter
}

// setEntity 集合数据结构实体
// set 采用值类型为空 map 构建
type setEntity struct {
	key       string
	container map[string]struct{}
}

// NewSetEntity 新建集合数据结构实体
func NewSetEntity(key string) Set {
	return &setEntity{
		key:       key,
		container: make(map[string]struct{}),
	}
}

// Add 找到插入失败， 否则插入
func (s *setEntity) Add(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 0
	}
	s.container[value] = struct{}{}
	return 1
}

// Exist 查找值是否存在，存在返回 1，否则返回 0
func (s *setEntity) Exist(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 1
	}
	return 0
}

// Rem 找到则删除，返回成功，否则返回 0
func (s *setEntity) Rem(value string) int64 {
	if _, ok := s.container[value]; ok {
		delete(s.container, value)
		return 1
	}
	return 0
}

// ToCmd 生成集合添加命令
func (s *setEntity) ToCmd() [][]byte {
	args := make([][]byte, 0, 2+len(s.container))
	args = append(args, []byte(def.CmdTypeSAdd), []byte(s.key))
	for k := range s.container {
		args = append(args, []byte(k))
	}

	return args
}
