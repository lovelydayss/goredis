package mlist

import def "github.com/lovelydayss/goredis/interface"

// List 链表类型操作接口
type List interface {
	LPush(value []byte)
	LPop(cnt int64) [][]byte
	RPush(value []byte)
	RPop(cnt int64) [][]byte
	Len() int64
	Range(start, stop int64) [][]byte
	def.CmdAdapter
}

// listEntity 链表元素结构体
// 这里就用数组替代实现了
type listEntity struct {
	key  string
	data [][]byte
}

// NewListEntity 初始化链表元素结构体
func NewListEntity(key string, elements ...[]byte) List {
	return &listEntity{
		key:  key,
		data: elements,
	}
}

func (l *listEntity) LPush(value []byte) {
	l.data = append([][]byte{value}, l.data...)
}

func (l *listEntity) LPop(cnt int64) [][]byte {
	if int64(len(l.data)) < cnt {
		return nil
	}

	poped := l.data[:cnt]
	l.data = l.data[cnt:]
	return poped
}

func (l *listEntity) RPush(value []byte) {
	l.data = append(l.data, value)
}

func (l *listEntity) RPop(cnt int64) [][]byte {
	if int64(len(l.data)) < cnt {
		return nil
	}

	poped := l.data[int64(len(l.data))-cnt:]
	l.data = l.data[:int64(len(l.data))-cnt]
	return poped
}

func (l *listEntity) Len() int64 {
	return int64(len(l.data))
}

func (l *listEntity) Range(start, stop int64) [][]byte {
	if stop == -1 {
		stop = int64(len(l.data) - 1)
	}

	if start < 0 || start >= int64(len(l.data)) {
		return nil
	}

	if stop < 0 || stop >= int64(len(l.data)) || stop < start {
		return nil
	}

	return l.data[start : stop+1]
}

func (l *listEntity) ToCmd() [][]byte {
	args := make([][]byte, 0, 2+l.Len())
	args = append(args, []byte(def.CmdTypeRPush), []byte(l.key))
	args = append(args, l.data...)
	return args
}
