package mbitmap

import (
	"strconv"

	def "github.com/lovelydayss/goredis/interface"
)

// BitMap 位图操作接口
type BitMap interface {
	Count() []byte
	SetBit(offset int64, val byte)
	GetBit(offset int64) []byte
	def.CmdAdapter
}

// BitMapEntity BitMap 实体
type BitMapEntity struct {
	key  string
	data []byte
}

// NewBitMapEntity 初始化
func NewBitMapEntity(key string) BitMap {

	return &BitMapEntity{
		key:  key,
		data: make([]byte, 0),
	}
}

func toByteSize(bitSize int64) int64 {
	if bitSize%8 == 0 {
		return bitSize / 8
	}
	return bitSize/8 + 1
}

// grow 扩容
func (b *BitMapEntity) grow(bitSize int64) {
	byteSize := toByteSize(bitSize)
	gap := byteSize - int64(len(b.data))
	if gap <= 0 {
		return
	}
	b.data = append(b.data, make([]byte, gap)...)
}

// Count 位图中 1 的位数
func (b *BitMapEntity) Count() []byte {

	// todo
	return []byte(strconv.FormatInt(int64(len(b.data)*8), 10))
}

// SetBit 设置位图某个位置的值
func (b *BitMapEntity) SetBit(offset int64, val byte) {

	byteIndex := offset / 8
	bitOffset := offset % 8
	mask := byte(1 << bitOffset)

	// 不需要扩容时返回空
	b.grow(offset + 1)

	// 设置第 byteIndex 个字节中 bitOffset 位的值为 val
	if val > 0 {
		// set bit
		(b.data)[byteIndex] |= mask
	} else {
		// clear bit
		(b.data)[byteIndex] &^= mask
	}
}

// GetBit 获取位图某个位置的值
func (b *BitMapEntity) GetBit(offset int64) []byte {
	byteIndex := offset / 8
	bitOffset := offset % 8

	if byteIndex >= int64(len(b.data)) {
		return nil
	}

	// 直接移位求解值
	res := ((b.data)[byteIndex] >> bitOffset) & 0x01
	return []byte(strconv.FormatInt(int64(res), 10))
}

// ToCmd 生成setbit指令
func (b *BitMapEntity) ToCmd() [][]byte {
	args := make([][]byte, 0, 2+2*len(b.data))
	args = append(args, []byte(def.CmdTypeBitmapSet), []byte(b.key), b.data)

	return args
}
