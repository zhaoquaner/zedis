package set

type Consumer func(member string) bool

type Set interface {
	Add(member string) int
	Remove(member string) int
	Contains(member string) bool
	Len() int
	Members() []string // 返回所有元素
	ForEach(consumer Consumer)
	RandomMembers(limit int) []string
	RandomDistinctMembers(limit int) []string
	Clear()
}
