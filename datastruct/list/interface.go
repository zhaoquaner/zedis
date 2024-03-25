package list

// Consumer 遍历list
// 接收index和value作为参数，返回true继续遍历，false停止遍历
type Consumer func(index int, v []byte) bool

// Expected 检查给定的item是否等于预期值，如果等于返回true
type Expected func(a []byte) bool

type List interface {
	AddFirst(val []byte) int          // 列表头添加元素
	AddLast(val []byte) int           // 列表末尾添加元素
	Get(index int) (val []byte)       // 根据所以返回元素
	Set(index int, val []byte) int    // 设置指定索引的元素
	Insert(index int, val []byte) int //向指定位置插入元素
	Remove(index int) (val []byte)    // 移除指定索引元素并返回
	RemoveFirst() (val []byte)        // 移除头元素并返回
	RemoveLast() (val []byte)         // 移除末尾元素并返回
	First() (val []byte)              // 返回头元素
	Last() (val []byte)               // 返回末尾元素
	Length() int                      // 返回列表长度
	ForEach(consumer Consumer)        // 遍历元素
	Contains(expected Expected) bool
	RemoveByValFromHead(val []byte, count int) int // 从表头开始，删除count个值为val的元素，返回实际删除的元素个数
	RemoveByValFromTail(val []byte, count int) int // 从表尾开始，删除count个值为val的元素，返回实际删除的元素个数
	RemoveAllByVal(val []byte) int                 // 删除列表中所有值为val的元素，返回实际删除的元素个数
}

// NewList 对外提供的新建list函数
func NewList(values [][]byte) *LinkedList {
	return newLinkedList(values)
}

// NewEmptyList 对外提供的新建list函数
func NewEmptyList() *LinkedList {
	return newEmptyLinkedList()
}
