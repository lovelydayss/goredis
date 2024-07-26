package mstring

import def "github.com/lovelydayss/goredis/interface"

// String 字符串类型接口
type String interface {
	Bytes() []byte
	def.CmdAdapter
}

// stringEntity 字符串类型实体
type stringEntity struct {
	key, str string
}

// NewString 初始化
func NewString(key, str string) String {
	return &stringEntity{key: key, str: str}
}

// Bytes 字节转换
func (s *stringEntity) Bytes() []byte {
	return []byte(s.str)
}

// ToCmd 转换为命令
func (s *stringEntity) ToCmd() [][]byte {
	return [][]byte{[]byte(def.CmdTypeSet), []byte(s.key), []byte(s.str)}
}
