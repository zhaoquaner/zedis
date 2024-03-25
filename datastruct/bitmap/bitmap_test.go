package bitmap

import (
	"fmt"
	"testing"
)

func TestBitMask(t *testing.T) {
	res := getBitMask(4)
	fmt.Printf("%08b", res)
}

func TestBitMap(t *testing.T) {
	bitmap := NewBitMap(nil)
	bitmap.SetBit(10, true)
	bitmap.SetBit(12, true)
	bitmap.SetBit(100, true)
	fmt.Printf("%v\n", bitmap.GetBit(10))
	fmt.Printf("%v\n", bitmap.GetBit(11))
	fmt.Printf("%v\n", bitmap.GetBit(12))
	fmt.Printf("%v\n", bitmap.GetBit(100))
}
