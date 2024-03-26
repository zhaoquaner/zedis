package db

// KeyEventCallback will be called back on key event, such as key inserted or deleted
// may be called concurrently
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

// DataEntity 存储与key绑定的数据value，包括字符串、列表、hash表、集合等
type DataEntity struct {
	Data any
	Type int // 数据类型
}

const (
	StringType = iota + 1
	ListType
	HashType
	SetType
	SortedType
)
