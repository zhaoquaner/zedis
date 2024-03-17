package dict

// Consumer 用于遍历Dict，如果返回false则遍历终止
type Consumer func(key string, val any) bool

// Dict 是k-v数据结构的接口定义
type Dict interface {
	Get(key string) (val any, exists bool)
	Exists(key string) bool
	Len() int
	Put(key string, val any) (result int)
	PutIfAbsent(key string, val any) (result int)
	PutIfExists(key string, val any) (result int)
	Remove(key string) (val any, result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string // 随机返回limit个key
	RandomDistinctKeys(limit int) []string
	Clear() // 情况
}
