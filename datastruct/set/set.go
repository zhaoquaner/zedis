package set

import "zedis/datastruct/dict"

type SimpleSet struct {
	dict dict.Dict
	v    struct{} // 所有key的共同value
}

// NewSet 使用初始元素创建集合
func NewSet(members ...string) *SimpleSet {
	set := &SimpleSet{
		dict: dict.NewSimpleDict(),
		v:    struct{}{},
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// Add 向集合添加元素，如果已存在，则覆盖
func (s *SimpleSet) Add(member string) int {
	return s.dict.Put(member, s.v)
}

func (s *SimpleSet) Remove(member string) int {
	_, ret := s.dict.Remove(member)
	return ret
}

func (s *SimpleSet) Contains(member string) bool {
	if s == nil || s.dict == nil {
		return false
	}
	return s.dict.Exists(member)
}

func (s *SimpleSet) Len() int {
	return s.dict.Len()
}

// Intersect 求多个集合交集
func Intersect(sets ...Set) Set {
	ret := NewSet()
	setNum := len(sets)
	if setNum == 0 {
		return ret
	}
	memberCountMap := make(map[string]int)
	for _, set := range sets {
		// 某一个set为空，直接返回空集合
		if set == nil || set.Len() == 0 {
			return NewSet()
		}
		set.ForEach(func(member string) bool {
			if _, ok := memberCountMap[member]; !ok {
				memberCountMap[member] = 0
			}
			memberCountMap[member] += 1
			return true
		})
	}
	for member, count := range memberCountMap {
		if count == setNum {
			ret.Add(member)
		}
	}
	return ret
}

// Diff 求第一个集合与其他集合差集，即set1相对于其他所有集合独有的元素
func Diff(sets ...Set) Set {
	setNum := len(sets)
	if setNum == 0 {
		return NewSet()
	}
	ret := NewSet(sets[0].Members()...)
	for i := 1; i < setNum; i++ {
		sets[i].ForEach(func(member string) bool {
			ret.Remove(member)
			return true
		})
		if ret.Len() == 0 {
			break
		}
	}
	return ret
}

// Union 求多个集合并集
func Union(sets ...Set) Set {
	ret := NewSet()
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			ret.Add(member)
			return true
		})
	}
	return ret
}

func (s *SimpleSet) Members() []string {
	return s.dict.Keys()
}

func (s *SimpleSet) ForEach(consumer Consumer) {
	s.dict.ForEach(func(key string, val any) bool {
		if !consumer(key) {
			return false
		}
		return true
	})
}

func (s *SimpleSet) RandomMembers(limit int) []string {
	return s.dict.RandomKeys(limit)
}

func (s *SimpleSet) RandomDistinctMembers(limit int) []string {
	return s.dict.RandomDistinctKeys(limit)
}

func (s *SimpleSet) Clear() {
	*s = *NewSet()
}
