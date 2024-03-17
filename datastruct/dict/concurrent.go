package dict

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m     map[string]any
	mutex sync.RWMutex
}

// 计算shard数量，至少为16，如果大于16，则找到一个大于等于param的最小的2的幂，因为哈希表容量通常为2的幂
func computeCapacity(param int) (size int) {
	if param < 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func NewConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m:     make(map[string]any),
			mutex: sync.RWMutex{},
		}
	}
	return &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 根据hash值获取shard索引
func (c *ConcurrentDict) spread(hashCode uint32) uint32 {
	if c == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(c.table))
	return (tableSize - 1) & hashCode
}

// getShard 根据索引获取shard
func (c *ConcurrentDict) getShard(index uint32) *shard {
	if c == nil {
		panic("dict is nil")
	}
	if index >= uint32(len(c.table)) {
		panic("index out of the table size")
	}
	return c.table[index]
}

func (c *ConcurrentDict) Get(key string) (val any, exists bool) {
	if c == nil {
		panic("dict is nil")
	}
	hash := fnv32(key)
	index := c.spread(hash)
	s := c.getShard(index)
	val, exists = s.m[key]
	return val, exists
}

func (c *ConcurrentDict) GetWithLock(key string) (val any, exists bool) {
	if c == nil {
		panic("dict is nil")
	}
	hash := fnv32(key)
	index := c.spread(hash)
	s := c.getShard(index)

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	val, exists = s.m[key]
	return val, exists
}

func (c *ConcurrentDict) Exists(key string) bool {
	_, ok := c.Get(key)
	return ok
}

func (c *ConcurrentDict) ExistsWithLock(key string) bool {
	_, ok := c.GetWithLock(key)
	return ok
}

func (c *ConcurrentDict) Len() int {
	if c == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&c.count))
}

func (c *ConcurrentDict) Put(key string, val any) (result int) {
	if c == nil {
		panic("dict is nil")
	}
	s := c.getShard(c.spread(fnv32(key)))
	_, exists := s.m[key]
	s.m[key] = val
	if exists {
		return 0
	}
	c.addCount()
	return 1
}

func (c *ConcurrentDict) PutWithLock(key string, val any) (result int) {
	if c == nil {
		panic("dict is nil")
	}
	s := c.getShard(c.spread(fnv32(key)))
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, exists := s.m[key]
	s.m[key] = val
	if exists {
		return 0
	}
	c.addCount()
	return 1
}

func (c *ConcurrentDict) PutIfAbsent(key string, val any) (result int) {
	if c == nil {
		panic("dict is nil")
	}
	s := c.getShard(c.spread(fnv32(key)))
	_, exists := s.m[key]
	if exists {
		return 0
	}
	s.m[key] = val
	c.addCount()
	return 1
}

func (c *ConcurrentDict) PutIfAbsentWithLock(key string, val any) (result int) {
	if c == nil {
		panic("dict is nil")
	}

	s := c.getShard(c.spread(fnv32(key)))
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.m[key]
	if exists {
		return 0
	}
	s.m[key] = val
	c.addCount()
	return 1
}

func (c *ConcurrentDict) PutIfExists(key string, val any) (result int) {
	if c == nil {
		panic("dict is nil")
	}

	s := c.getShard(c.spread(fnv32(key)))
	_, exists := s.m[key]
	if exists {
		s.m[key] = val
		return 1
	}
	return 0
}

func (c *ConcurrentDict) PutIfExistsWithLock(key string, val any) (result int) {
	if c == nil {
		panic("dict is nil")
	}

	s := c.getShard(c.spread(fnv32(key)))
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.m[key]
	if exists {
		s.m[key] = val
		return 1
	}
	return 0
}

func (c *ConcurrentDict) Remove(key string) (val any, result int) {
	if c == nil {
		panic("dict is nil")
	}

	s := c.getShard(c.spread(fnv32(key)))
	v, exists := s.m[key]
	if !exists {
		return nil, 0
	}
	delete(s.m, key)
	c.decreaseCount()
	return v, 1

}

func (c *ConcurrentDict) RemoveWithLock(key string) (val any, result int) {
	if c == nil {
		panic("dict is nil")
	}

	s := c.getShard(c.spread(fnv32(key)))
	s.mutex.Lock()
	defer s.mutex.Unlock()

	v, exists := s.m[key]
	if !exists {
		return nil, 0
	}
	delete(s.m, key)
	c.decreaseCount()
	return v, 1
}

func (c *ConcurrentDict) ForEach(consumer Consumer) {
	if c == nil {
		panic("dict is nil")
	}

	for _, s := range c.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for k, v := range s.m {
				if !consumer(k, v) {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

func (c *ConcurrentDict) Keys() []string {
	keys := make([]string, c.Len())
	i := 0
	c.ForEach(func(key string, val any) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})

	return keys
}

func (c *ConcurrentDict) RandomKeys(limit int) []string {
	return nil
}

func (c *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	return nil
}

func (c *ConcurrentDict) Clear() {
	*c = *NewConcurrentDict(c.shardCount)
}

func (c *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&c.count, 1)
}

func (c *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&c.count, -1)
}

func (c *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	// 为了去重
	for _, key := range keys {
		index := c.spread(fnv32(key))
		indexMap[index] = struct{}{}
	}

	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}

	sort.SliceIsSorted(indices, func(i, j int) bool {
		if reverse {
			return indices[i] > indices[j]
		}
		return indices[i] < indices[j]
	})
	return indices
}

// RWLocks 根据写key和读key，分别lock 写锁和读锁，允许重复key
func (c *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := c.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		idx := c.spread(fnv32(key))
		writeIndexSet[idx] = struct{}{}
	}

	for _, index := range indices {
		_, ok := writeIndexSet[index]
		mu := &c.getShard(index).mutex
		if ok {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}

}

// RWUnLocks 根据写key和读key，分别unlock 写锁和读锁，允许重复key
func (c *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := c.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		idx := c.spread(fnv32(key))
		writeIndexSet[idx] = struct{}{}
	}

	for _, index := range indices {
		_, ok := writeIndexSet[index]
		mu := &c.getShard(index).mutex
		if ok {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}

}
