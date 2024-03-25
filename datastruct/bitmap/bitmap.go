package bitmap

type BitMap []byte

func NewBitMap(data []byte) *BitMap {
	b := BitMap(data)
	return &b
}

func NewEmptyBitMap() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

// grow 根据偏移量扩容字节数组，如果当前容量能够满足要求，则不执行扩容操作
func (b *BitMap) grow(offset int64) {
	byteIndex := toByteIndex(offset)
	if b.ByteSize() >= byteIndex+1 {
		return
	}
	newSize := byteIndex + 1
	*b = append(*b, make([]byte, newSize-b.ByteSize())...)
}

func (b *BitMap) GetBit(offset int64) bool {
	if offset >= b.BitSize() {
		return false
	}
	byteIndex := toByteIndex(offset)
	mask := getBitMask(toBitIndex(offset))
	return (*b)[byteIndex]&mask > 0
}

func (b *BitMap) SetBit(offset int64, value bool) {
	b.grow(offset)
	byteIndex := toByteIndex(offset)
	mask := getBitMask(toBitIndex(offset))
	if value {
		(*b)[byteIndex] |= mask
	} else {
		(*b)[byteIndex] &= ^mask
	}
}

func (b *BitMap) ForEachBit(begin int64, end int64, consumer BitConsumer) {
	if b == nil {
		panic("bit map is nil")
	}
	if b.ByteSize() == 0 {
		return
	}

	if end > b.BitSize() {
		end = b.BitSize()
	}

	for i := begin; i < end; i++ {
		if !consumer(i, b.GetBit(i)) {
			break
		}
	}
}

func (b *BitMap) ForEachByte(begin int64, end int64, consumer ByteConsumer) {
	if b == nil {
		panic("bit map is nil")
	}
	if b.ByteSize() == 0 {
		return
	}
	if end > b.ByteSize() {
		end = b.ByteSize()
	}
	for i := begin; i < end; i++ {
		if !consumer(i, (*b)[i]) {
			break
		}
	}

}

func (b *BitMap) BitSize() int64 {
	return int64(len(*b) * 8)
}

func (b *BitMap) ByteSize() int64 {
	return int64(len(*b))
}

// toByteIndex 根据bit大小返回字节数组索引(从0开始)
func toByteIndex(bitSize int64) int64 {
	return bitSize / 8
}

// toBitIndex 根据bit大小返回 一个字节内 从左到右 数 对应索引, 范围为 [1, 8]
func toBitIndex(bitSize int64) int64 {
	return bitSize%8 + 1
}

// getBitMask 根据bit索引，获取对应的掩码，例如 10000000、00100000、00000010
func getBitMask(bitIndex int64) byte {
	res := 0b10000000
	return byte(res >> (bitIndex - 1))
}
