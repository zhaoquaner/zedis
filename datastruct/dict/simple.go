package dict

type SimpleDict struct {
	m map[string]any
}

func NewSimpleDict() *SimpleDict {
	return &SimpleDict{m: make(map[string]any)}
}

func (s *SimpleDict) Get(key string) (val any, exists bool) {
	v, ok := s.m[key]
	return v, ok
}

func (s *SimpleDict) Exists(key string) bool {
	_, ok := s.m[key]
	return ok
}

func (s *SimpleDict) Len() int {
	if s.m == nil {
		panic("map is nil")
	}
	return len(s.m)
}

// Put 将key value存入map，如果key已存在，则更新；返回新建kv的数量
func (s *SimpleDict) Put(key string, val any) (result int) {
	_, existed := s.m[key]
	s.m[key] = val
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent 在key不存在的情况下，才贵存入value，并返回更新key-value键值对数量
func (s *SimpleDict) PutIfAbsent(key string, val any) (result int) {
	_, existed := s.m[key]
	if existed {
		return 0
	}
	s.m[key] = val
	return 1
}

// PutIfExists 在key存在的情况下，更新value，并返回更新数量
func (s *SimpleDict) PutIfExists(key string, val any) (result int) {
	_, existed := s.m[key]
	if !existed {
		return 0
	}
	s.m[key] = val
	return 1
}

// Remove 移除key-value键值对，并返回被删除的value以及删除数量
func (s *SimpleDict) Remove(key string) (val any, result int) {
	val, existed := s.m[key]
	delete(s.m, key)
	if existed {
		return val, 1
	}
	return nil, 0
}

// ForEach 遍历map，如果consumer返回false，终止遍历
func (s *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range s.m {
		if !consumer(k, v) {
			break
		}
	}
}

// Keys 返回所有key
func (s *SimpleDict) Keys() []string {
	result := make([]string, len(s.m))
	idx := 0
	for k := range s.m {
		result[idx] = k
		idx++
	}
	return result
}

// RandomKeys 随机返回给定数量的key
func (s *SimpleDict) RandomKeys(limit int) []string {
	size := limit
	if size > s.Len() {
		size = s.Len()
	}
	result := make([]string, size)
	for i := 0; i < size; i++ {
		for k := range s.m {
			result[i] = k
			break
		}
	}
	return result
}

func (s *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(s.m) {
		size = len(s.m)
	}
	result := make([]string, size)
	i := 0
	for k := range s.m {
		if i == size {
			break
		}
		result[i] = k
		i++
	}
	return result
}

func (s *SimpleDict) Clear() {
	*s = *NewSimpleDict()
}
