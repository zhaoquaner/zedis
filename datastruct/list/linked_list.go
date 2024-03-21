package list

type LinkedList struct {
	head   *node
	tail   *node
	length int
}

type node struct {
	val  []byte
	prev *node
	next *node
}

func NewEmptyLinkedList() *LinkedList {
	return NewLinkedList(nil)
}

func NewLinkedList(values [][]byte) *LinkedList {
	headNode := &node{
		val: nil,
	}
	tailNode := &node{
		val: nil,
	}
	headNode.next = tailNode
	tailNode.prev = headNode
	list := &LinkedList{
		head:   headNode,
		tail:   tailNode,
		length: 0,
	}
	for _, val := range values {
		list.AddLast(val)
	}
	return list
}

func (l *LinkedList) AddFirst(val []byte) int {
	newNode := &node{
		val:  val,
		prev: l.head,
		next: l.head.next,
	}
	l.head.next.prev = newNode
	l.head.next = newNode
	l.length++
	return 1
}

func (l *LinkedList) AddLast(val []byte) int {
	newNode := &node{
		val:  val,
		prev: l.tail.prev,
		next: l.tail,
	}
	l.tail.prev.next = newNode
	l.tail.prev = newNode
	l.length++
	return 1
}

// index范围为0 ~ l.length - 1
func (l *LinkedList) find(index int) (n *node) {
	if l.length == 0 || (index < 0 || index >= l.length) {
		return nil
	}
	if index <= l.length/2 {
		cur := l.head.next
		for i := 0; i < index; i++ {
			cur = cur.next
		}
		return cur
	} else {
		cur := l.tail.prev
		for i := 0; i < l.length-index-1; i++ {
			cur = cur.prev
		}
		return cur
	}
}

// Get index从0开始
func (l *LinkedList) Get(index int) (val []byte) {
	n := l.find(index)
	if n != nil {
		return n.val
	}
	return nil
}

func (l *LinkedList) Set(index int, val []byte) int {
	n := l.find(index)
	if n == nil {
		return 0
	}
	n.val = val
	return 1
}

func (l *LinkedList) Insert(index int, val []byte) int {
	if index == 0 {
		l.AddFirst(val)
		return 1
	} else if index == l.length {
		l.AddLast(val)
		return 1
	}
	n := l.find(index)
	if n == nil {
		return 0
	}
	newNode := &node{
		val:  val,
		prev: n.prev,
		next: n,
	}
	n.prev.next = newNode
	n.prev = n
	l.length++
	return 1
}

func (l *LinkedList) Remove(index int) (val []byte) {
	n := l.find(index)
	if n == nil {
		return nil
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	n.next = nil
	n.prev = nil // for Go garbage collection
	l.length--
	return n.val
}

func (l *LinkedList) RemoveFirst() (val []byte) {
	return l.Remove(0)
}

func (l *LinkedList) RemoveLast() (val []byte) {
	return l.Remove(l.length - 1)
}

func (l *LinkedList) First() (val []byte) {
	if l.length > 0 {
		return l.head.next.val
	}
	return nil
}

func (l *LinkedList) Last() (val []byte) {
	if l.length > 0 {
		return l.tail.prev.val
	}
	return nil
}

func (l *LinkedList) Length() int {
	return l.length
}

func (l *LinkedList) ForEach(consumer Consumer) {
	if l.length == 0 {
		return
	}
	idx := 0
	cur := l.head.next
	for cur != l.tail {
		if !consumer(idx, cur.val) {
			break
		}
		cur = cur.next
		idx++
	}
}

func (l *LinkedList) Contains(expected Expected) bool {
	if l.length == 0 {
		return false
	}
	contain := false
	l.ForEach(func(index int, v []byte) bool {
		if expected(v) {
			contain = true
			return false
		}
		return true
	})
	return contain
}
