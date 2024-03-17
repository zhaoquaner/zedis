package list

// Consumer 遍历list
// 接收index和value作为参数，返回true继续遍历，false停止遍历
type Consumer func(index int, v any) bool

type List interface {
	Length() int
	Add(any any) (result int)
	Get(index int) (val any)
	Set(index int, val any)
	Insert(index int, any any)
	Remove(index int) (val any)
	First() (any any)
	Last() (any any)
}
