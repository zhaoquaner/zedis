package bitmap

type BitConsumer func(offset int64, val bool) bool
type ByteConsumer func(offset int64, val byte) bool

type IBitMap interface {
	GetBit(offset int64) bool
	SetBit(offset int64, value bool)
	ForEachBit(begin int64, end int64, consumer BitConsumer)   // 遍历每个bit，begin和end是bit索引(从0开始，begin包括，end不包括)
	ForEachByte(begin int64, end int64, consumer ByteConsumer) // 遍历每个字节，begin和end是字节位置索引(从0开始，begin包括，end不包括)
	BitSize() int64
	ByteSize() int64
}
