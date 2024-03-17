package dict

import (
	"fmt"
	"testing"
)

func TestComputeCapacity(t *testing.T) {
	fmt.Printf("%v", computeCapacity(19))
}

func TestConcurrentDict(t *testing.T) {
	dict := NewConcurrentDict(30)
	dict.Put("key1", "value1")
	dict.Put("key2", "value2")
	dict.Put("key3", "value3")
	dict.Put("key4", "value4")
	dict.Put("key5", "value5")

	keys := dict.Keys()
	for _, v := range keys {
		fmt.Printf("%s\n", v)
	}

}
