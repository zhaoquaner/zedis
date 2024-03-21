package list

import (
	"fmt"
	"testing"
)

func TestConvertByteArray(t *testing.T) {
	var num int64 = 1000000
	byteArray := convertToByteArray(int64(uint64(num)))
	for _, b := range byteArray {
		fmt.Printf("%08b ", b)
	}
}

func TestGenerateEntry(t *testing.T) {
	//bytes := buildEntry(255, []byte{'a', 'b', 'c', 'd'})
	//for _, b := range bytes {
	//	fmt.Printf("%d ", b)
	//}
}

func TestLinkedList(t *testing.T) {
	l := NewEmptyLinkedList()
	l.AddLast([]byte("3"))
	l.AddLast([]byte("4"))
	l.AddLast([]byte("5"))
	l.AddLast([]byte("6"))
	l.AddFirst([]byte("1"))
	l.AddFirst([]byte("0"))
	l.Insert(2, []byte("2"))

	l.RemoveFirst()
	l.RemoveLast()
	l.Remove(4)

	l.ForEach(func(index int, v []byte) bool {
		fmt.Printf("%v ", string(v))
		return true
	})

	fmt.Printf("\n%v", string(l.Last()))
	fmt.Printf("\n%v", string(l.First()))

	fmt.Printf("\n%v", l.Contains(func(a []byte) bool {
		return string(a) == "2"
	}))

	//fmt.Printf("\n%v", string(l.Get()))
}

func TestToInt(t *testing.T) {

	fmt.Printf("%d\n", 0b1111000000001111)

	fmt.Printf("%v", toInt([]byte{0xF0, 0x0F}))
}
