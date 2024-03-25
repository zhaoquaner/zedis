package database

import (
	"errors"
	"github.com/duke-git/lancet/v2/mathutil"
	"strings"
	"time"
	"zedis/datastruct/list"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/redis/protocol"
)

// KeyValue 用于阻塞命令 的channel数据传输
type KeyValue struct {
	key   []byte
	value []byte
}

// KeyValues 用于阻塞命令 的channel数据传输
type KeyValues struct {
	key    []byte
	values [][]byte
}

func buildListEntity(data list.List) *db.DataEntity {
	return &db.DataEntity{
		Data: data,
		Type: db.ListType,
	}
}

func (d *DB) getEntityAsList(key string) (list.List, redis.Reply) {
	entity, exists := d.GetEntity(key)
	if !exists {
		return nil, nil
	}
	if entity.Type != db.ListType {
		return nil, protocol.ErrorWrongTypeReply
	}
	return entity.Data.(list.List), nil
}

// LPushCommand 向列表头插入元素，返回插入后的列表长度
// LPUSH key element [element ...]
// key不存在，创建一个空列表
// 类型不是list，返回错误
func LPushCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		l = list.NewList(args[1:])
		d.PutEntity(key, buildListEntity(l))
	} else {
		for _, arg := range args[1:] {
			l.AddFirst(arg)
		}
	}
	return protocol.NewIntReply(int64(l.Length()))
}

// LPushXCommand 向列表头插入元素，返回插入后的列表长度；与LPUSH命令不同的是，如果key不存在，则不执行任务操作
// LPUSHX key element [element ...]
// key不存在，不执行操作
// 类型不是list，返回错误
func LPushXCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ZeroReply
	} else {
		for _, arg := range args[1:] {
			l.AddFirst(arg)
		}
	}
	return protocol.NewIntReply(int64(l.Length()))
}

// RPushCommand 向列表末尾插入元素，返回插入后的列表长度
// RPUSH key element [element ...]
// key不存在，创建一个空列表
// 类型不是list，返回错误
func RPushCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		l = list.NewList(args[1:])
		d.PutEntity(key, buildListEntity(l))
	} else {
		for _, arg := range args[1:] {
			l.AddLast(arg)
		}
	}
	return protocol.NewIntReply(int64(l.Length()))
}

// RPushXCommand 向列表末尾插入元素，返回插入后的列表长度；与RPUSH命令不同的是，如果key不存在，则不执行任务操作
// RPUSHX key element [element ...]
// key不存在，不执行操作
// 类型不是list，返回错误
func RPushXCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ZeroReply
	} else {
		for _, arg := range args[1:] {
			l.AddLast(arg)
		}
	}
	return protocol.NewIntReply(int64(l.Length()))
}

// LPopCommand 从列表头弹出元素并返回，count指定弹出的数量
// LPOP key [count]
// key不存在，返回Nil
// 只返回一个元素，Bulk string；多个元素，Multi Bulk String
// 类型不是list，返回错误
func LPopCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 2 {
		return protocol.NewArgNumErrReply("LPOP")
	} else if len(args) == 2 {
		var err error
		count, err = parseInt(args[1])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
	}

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.NullBulkReply
	}
	if l.Length() < count {
		count = l.Length()
	}
	values := make([][]byte, 0)
	for i := 0; i < count; i++ {
		values = append(values, l.RemoveFirst())
	}
	if l.Length() == 0 {
		d.Remove(key)
	}
	return protocol.NewMultiBulkReply(values)
}

// BLPopCommand 从第一个非空列表的表头弹出元素，并返回一个数组，第一个元素是非空的key，第二个是弹出的元素值；如果所有key都不存在，则阻塞该客户端的连接
// 直到某一个key对应list插入了元素；最后一个参数指定以秒为单位的超时时间，如果是0，则表示不设置超时时间
// BLPOP key [key ...] timeout
func BLPopCommand(d *DB, args [][]byte) redis.Reply {
	timeout, err := parseInt(args[len(args)-1])
	if err != nil {
		return protocol.NewErrorReply("ERR timeout is not an integer or out of range")
	}
	if timeout < 0 {
		return protocol.NewErrorReply("ERR timeout is negative")
	}

	numKeys := len(args) - 1
	keys := make([]string, 0)
	for i := 0; i < numKeys; i++ {
		key := string(args[i])
		l, errReply := d.getEntityAsList(key)
		if errReply != nil {
			return errReply
		}
		if l != nil {
			val := l.RemoveFirst()
			if l.Length() == 0 {
				d.Remove(key)
			}
			return protocol.NewMultiBulkReply([][]byte{args[i], val})
		}
		keys = append(keys, key)
	}

	kvChan := make(chan *KeyValue)

	go func(d *DB, kvChan chan *KeyValue) {
		timeTicker := time.Tick(time.Duration(timeout) * time.Second)
		idx := 0
		for {
			select {
			case <-timeTicker:
				kvChan <- nil
				return
			default:
				if idx == len(keys) {
					idx = 0
				}

				kv := func() *KeyValue {
					key := keys[idx]

					d.RWLocks([]string{key}, nil)
					defer d.RWUnLocks([]string{key}, nil)

					l, _ := d.getEntityAsList(key)
					if l != nil {
						val := l.RemoveFirst()
						if l.Length() == 0 {
							d.Remove(key)
						}
						return &KeyValue{key: []byte(key), value: val}
					}
					return nil

				}()

				if kv != nil {
					kvChan <- kv
					return
				}

				idx++
			}
		}

	}(d, kvChan)

	kv := <-kvChan
	if kv == nil {
		return protocol.NullBulkReply
	} else {
		return protocol.NewMultiBulkReply([][]byte{kv.key, kv.value})
	}
}

// RPopCommand 从列表末尾弹出元素并返回，count指定弹出的数量
// RPOP key [count]
// key不存在，返回Nil
// 只返回一个元素，Bulk string；多个元素，Multi Bulk String
// 类型不是list，返回错误
func RPopCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 2 {
		return protocol.NewArgNumErrReply("RPOP")
	} else if len(args) == 2 {
		var err error
		count, err = parseInt(args[1])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
	}

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.NullBulkReply
	}
	if l.Length() < count {
		count = l.Length()
	}
	values := make([][]byte, 0)
	for i := 0; i < count; i++ {
		values = append(values, l.RemoveLast())
	}
	if l.Length() == 0 {
		d.Remove(key)
	}
	return protocol.NewMultiBulkReply(values)
}

// BRPopCommand 从第一个非空列表的末尾弹出元素，并返回一个数组，第一个元素是非空的key，第二个是弹出的元素值；如果所有key都不存在，则阻塞该客户端的连接
// 直到某一个key对应list插入了元素；最后一个参数指定以秒为单位的超时时间，如果是0，则表示不设置超时时间
// BRPOP key [key ...] timeout
func BRPopCommand(d *DB, args [][]byte) redis.Reply {
	timeout, err := parseInt(args[len(args)-1])
	if err != nil {
		return protocol.NewErrorReply("ERR timeout is not an integer or out of range")
	}
	if timeout < 0 {
		return protocol.NewErrorReply("ERR timeout is negative")
	}

	numKeys := len(args) - 1
	keys := make([]string, 0)
	for i := 0; i < numKeys; i++ {
		key := string(args[i])
		l, errReply := d.getEntityAsList(key)
		if errReply != nil {
			return errReply
		}
		if l != nil {
			val := l.RemoveFirst()
			if l.Length() == 0 {
				d.Remove(key)
			}
			return protocol.NewMultiBulkReply([][]byte{args[i], val})
		}
		keys = append(keys, key)
	}

	kvChan := make(chan *KeyValue)

	go func(d *DB, kvChan chan *KeyValue) {
		timeTicker := time.Tick(time.Duration(timeout) * time.Second)
		idx := 0
		for {
			select {
			case <-timeTicker:
				kvChan <- nil
				return
			default:
				if idx == len(keys) {
					idx = 0
				}

				kv := func() *KeyValue {
					key := keys[idx]

					d.RWLocks([]string{key}, nil)
					defer d.RWUnLocks([]string{key}, nil)

					l, _ := d.getEntityAsList(key)
					if l != nil {
						val := l.RemoveLast()
						if l.Length() == 0 {
							d.Remove(key)
						}
						return &KeyValue{key: []byte(key), value: val}
					}
					return nil

				}()

				if kv != nil {
					kvChan <- kv
					return
				}

				idx++
			}
		}

	}(d, kvChan)

	kv := <-kvChan
	if kv == nil {
		return protocol.NullBulkReply
	} else {
		return protocol.NewMultiBulkReply([][]byte{kv.key, kv.value})
	}
}

// LLenCommand 返回list长度
// LLEN key
func LLenCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ZeroReply
	}

	return protocol.NewIntReply(int64(l.Length()))
}

// LIndexCommand 返回指定index对应的value
// LINDEX key index
// 索引可为负，表示从末尾往前
// 索引越界返回null
func LIndexCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index, err := parseInt(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	length := l.Length()
	if index < 0 {
		index = length + index
	}
	if index < 0 || index > length-1 {
		return protocol.NullBulkReply
	}
	return protocol.NewBulkReply(l.Get(index))
}

// LRangeCommand 遍历列表元素
// LRANGE key start stop
// 索引可为负数
// start和stop都包括，例如 0 10，会返回索引0到10这11个元素
// 索引越界不会报错
// 如果start大于list长度，则返回空数组
// 如果stop大于长度，则认为到末尾结束
func LRangeCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start, err := parseInt(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	end, err := parseInt(args[2])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil || l.Length() == 0 {
		return protocol.EmptyMultiBulkReply
	}
	length := l.Length()

	start, err = adjustIndex(l.Length(), start)
	if err != nil {
		return protocol.EmptyMultiBulkReply
	}
	end, err = adjustIndex(l.Length(), end)
	if err != nil {
		end = length - 1
	}

	if start > end {
		return protocol.EmptyMultiBulkReply
	}

	res := make([][]byte, 0)
	l.ForEach(func(index int, v []byte) bool {
		if index > end {
			return false
		}
		if index >= start && index <= end {
			res = append(res, v)
		}
		return true
	})
	return protocol.NewMultiBulkReply(res)
}

// LInsertCommand 向指定的元素 前/后插入元素
// LINSERT key <BEFORE | AFTER> pivot element
// 即在pivot元素 前/后 插入element，注意pivot不是索引，而是元素值
// 如果key不存在 或pivot不存在，则不执行任何操作
// key不存在，返回0
// pivot不存在，返回-1
// 插入成功，返回插入后列表长度
func LInsertCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	location := strings.ToLower(string(args[1]))
	if location != "before" && location != "after" {
		return protocol.ErrorSyntaxReply
	}
	pivot := string(args[2])
	element := args[3]
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ZeroReply
	}
	index := -1
	l.ForEach(func(idx int, v []byte) bool {
		if string(v) == pivot {
			index = idx
			return false
		}
		return true
	})

	if index == -1 {
		return protocol.NewIntReply(-1)
	}
	if location == "after" {
		l.Insert(index+1, element)
	} else {
		l.Insert(index, element)
	}

	return protocol.NewIntReply(int64(l.Length()))
}

// LRemCommand 从list删除指定count个指定的element，如果count > 0，则从头开始遍历删除；count < 0, 则从末尾开始遍历删除; count = 0,删除全部值等于element的元素
// LREM key count element
// 如果key不存在，则视为空列表
// 返回值为实际删除的元素个数
func LRemCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count, err := parseInt(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	element := args[2]
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil || l.Length() == 0 {
		return protocol.ZeroReply
	}
	deletedCount := 0
	if count == 0 {
		deletedCount = l.RemoveAllByVal(element)
	} else if count > 0 {
		deletedCount = l.RemoveByValFromHead(element, count)
	} else {
		deletedCount = l.RemoveByValFromTail(element, -count)
	}
	if l.Length() == 0 {
		d.Remove(key)
	}
	return protocol.NewIntReply(int64(deletedCount))
}

// LSetCommand list指定索引的元素修改为element,修改成功,返回OK
// LSET key index element
// 如果索引越界，返回错误
func LSetCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index, err := parseInt(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	element := args[2]
	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ErrorIndexOutOfRangeReply
	}

	if index < 0 {
		index += l.Length()
	}
	if index < 0 || index > l.Length()-1 {
		return protocol.ErrorIndexOutOfRangeReply
	}
	l.Set(index, element)
	return protocol.OKReply
}

// LTrimCommand 删除list索引 0 ~ start - 1和 stop + 1 ~ length - 1的元素，只剩下 start ~ stop这个子列表
// LTRIM key start stop
// 索引可以为负值，越界不会报错
// 如果start小于0，则认为是0；如果start 大于length - 1，则认为是空数组，删去该key
// 如果end大于length - 1，则任务是length - 1
// 如果 start > end，则认为是空数组，删去该key
// 设置成功返回OK
func LTrimCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start, err := parseInt(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	stop, err := parseInt(args[2])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ErrorNoSuchKeyReply
	}
	length := l.Length()
	start, err = adjustIndex(length, start)
	if err != nil {
		d.Remove(key)
		return protocol.OKReply
	}
	stop, err = adjustIndex(length, stop)
	if err != nil {
		stop = length - 1
	}
	if start > stop {
		d.Remove(key)
		return protocol.OKReply
	}

	for i := 0; i < start; i++ {
		l.RemoveFirst()
	}
	for i := 0; i < length-1-stop; i++ {
		l.RemoveLast()
	}
	return protocol.OKReply
}

// LMoveCommand 将source的第一个/最后一个 拿出来，放到destination 列表头/尾，返回操作的元素值
// LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>
// 如果source 不存在，返回nil
// source和destination可以为同一个key
func LMoveCommand(d *DB, args [][]byte) redis.Reply {
	source := string(args[0])
	dest := string(args[1])

	sourceLoc := strings.ToLower(string(args[2]))
	destLoc := strings.ToLower(string(args[3]))
	if sourceLoc != "left" && sourceLoc != "right" {
		return protocol.ErrorSyntaxReply
	}
	if destLoc != "left" && destLoc != "right" {
		return protocol.ErrorSyntaxReply
	}

	sourceList, errReply := d.getEntityAsList(source)
	if errReply != nil {
		return errReply
	}
	var destList list.List
	if source == dest {
		destList = sourceList
	} else {
		destList, errReply = d.getEntityAsList(dest)
		if errReply != nil {
			return errReply
		}
	}

	if sourceList == nil {
		return protocol.NullBulkReply
	}
	if destList == nil {
		destList = list.NewEmptyList()
		d.PutEntity(dest, buildListEntity(destList))
	}

	var val []byte
	if sourceLoc == "left" {
		val = sourceList.RemoveFirst()
	} else {
		val = sourceList.RemoveLast()
	}

	if sourceList.Length() == 0 {
		d.Remove(source)
	}

	if destLoc == "left" {
		destList.AddFirst(val)
	} else {
		destList.AddLast(val)
	}

	return protocol.NewBulkReply(val)
}

// BLMoveCommand 将source的第一个/最后一个 拿出来，放到destination 列表头/尾，返回操作的元素值
// BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
// 如果source 不存在，返回nil
// source和destination可以为同一个key
func BLMoveCommand(d *DB, args [][]byte) redis.Reply {
	source := string(args[0])
	dest := string(args[1])

	sourceLoc := strings.ToLower(string(args[2]))
	destLoc := strings.ToLower(string(args[3]))
	if (sourceLoc != "left" && sourceLoc != "right") || (destLoc != "left" && destLoc != "right") {
		return protocol.ErrorSyntaxReply
	}
	timeout, err := parseInt(args[4])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	if timeout < 0 {
		return protocol.NewErrorReply("ERR timeout is negative")
	}

	d.RWLocks([]string{source, dest}, nil)

	sourceList, errReply := d.getEntityAsList(source)
	if errReply != nil {
		return errReply
	}
	var destList list.List
	if source == dest {
		destList = sourceList
	} else {
		destList, errReply = d.getEntityAsList(dest)
		if errReply != nil {
			return errReply
		}
	}

	if sourceList != nil {
		if destList == nil {
			destList = list.NewEmptyList()
			d.PutEntity(dest, buildListEntity(destList))
		}

		var val []byte
		if sourceLoc == "left" {
			val = sourceList.RemoveFirst()
		} else {
			val = sourceList.RemoveLast()
		}

		if sourceList.Length() == 0 {
			d.Remove(source)
		}

		if destLoc == "left" {
			destList.AddFirst(val)
		} else {
			destList.AddLast(val)
		}

		d.RWUnLocks([]string{source, dest}, nil)
		return protocol.NewBulkReply(val)
	} else {
		d.RWUnLocks([]string{source, dest}, nil)
	}

	valChan := make(chan []byte)
	go func(d *DB, valChan chan []byte) {
		timeTicker := time.Tick(time.Duration(timeout) * time.Second)
		for {
			select {
			case <-timeTicker:
				valChan <- nil
				return
			default:
				val := func() []byte {
					d.RWLocks([]string{source}, nil)
					defer d.RWUnLocks([]string{source}, nil)

					l, _ := d.getEntityAsList(source)
					if l == nil {
						return nil
					}

					var val []byte
					if sourceLoc == "left" {
						val = l.RemoveFirst()
					} else {
						val = l.RemoveLast()
					}
					if l.Length() == 0 {
						d.Remove(source)
					}
					return val
				}()
				if val != nil {
					valChan <- val
					return
				}

			}
		}

	}(d, valChan)

	val := <-valChan
	if val == nil {
		return protocol.NullBulkReply
	}

	d.RWLocks([]string{dest}, nil)
	defer d.RWUnLocks([]string{dest}, nil)

	if destList == nil {
		destList = list.NewEmptyList()
		d.PutEntity(dest, buildListEntity(destList))
	}
	if destLoc == "left" {
		destList.AddFirst(val)
	} else {
		destList.AddLast(val)
	}
	return protocol.NewBulkReply(val)
}

// LMPopCommand 根据传递的参数，从第一个非空列表的左侧或者右侧弹出元素，弹出的数量是count(默认为1)和列表长度的较小值
// LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
// 返回格式为：第一行为非空列表对应的key； 后面是弹出的元素列表
// 注意：虽然给定了多个key，但是只从第一个遇到的非空列表弹出，后面的key就不再处理
func LMPopCommand(d *DB, args [][]byte) redis.Reply {
	numkeys, err := parseInt(args[0])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	if numkeys <= 0 {
		return protocol.NullBulkReply
	}
	if len(args) < 2+numkeys || len(args) > 4+numkeys {
		return protocol.ErrorSyntaxReply
	}
	loc := strings.ToLower(string(args[1+numkeys]))
	if loc != "left" && loc != "right" {
		return protocol.ErrorSyntaxReply
	}
	count := 0
	if len(args) == 3+numkeys {
		count, err = parseInt(args[2+numkeys])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
		if count <= 0 {
			return protocol.NullBulkReply
		}
	}

	var l list.List
	var key string
	var errReply redis.Reply
	for i := 1; i < numkeys+1; i++ {
		l, errReply = d.getEntityAsList(string(args[i]))
		if errReply != nil {
			return errReply
		}
		if l != nil {
			key = string(args[i])
			break
		}
	}

	if l == nil {
		return protocol.NullBulkReply
	}

	writeKeys := []string{key}
	d.RWLocks(writeKeys, nil)
	defer d.RWUnLocks(writeKeys, nil)

	count = mathutil.Min(count, l.Length())

	values := make([][]byte, 0)
	for i := 0; i < count; i++ {
		if loc == "left" {
			values = append(values, l.RemoveFirst())
		} else {
			values = append(values, l.RemoveLast())
		}
	}
	if l.Length() == 0 {
		d.Remove(key)
	}

	res := protocol.NewArrayReply([]redis.Reply{protocol.NewBulkReply([]byte(key)), protocol.NewMultiBulkReply(values)})
	return res
}

// BLMPopCommand 根据传递的参数，从第一个非空列表的左侧或者右侧弹出元素，弹出的数量是count(默认为1)和列表长度的较小值；如果所有key的list都为空，则阻塞至超时或领一个客户端向其中一个key push元素
// BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
// 返回格式为：第一行为非空列表对应的key； 后面是弹出的元素列表
// 注意：虽然给定了多个key，但是只从第一个遇到的非空列表弹出，后面的key就不再处理
// 如果timeout超时，则返回null
func BLMPopCommand(d *DB, args [][]byte) redis.Reply {
	timeout, err := parseInt(args[0])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	if timeout < 0 {
		return protocol.NewErrorReply("ERR timeout is negative")
	}
	numkeys, err := parseInt(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	if numkeys <= 0 {
		return protocol.NullBulkReply
	}
	if len(args) < 3+numkeys || len(args) > 5+numkeys {
		return protocol.ErrorSyntaxReply
	}
	loc := strings.ToLower(string(args[2+numkeys]))
	if loc != "left" && loc != "right" {
		return protocol.ErrorSyntaxReply
	}
	count := 1
	if len(args) == 5+numkeys {
		count, err = parseInt(args[4+numkeys])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
		if count <= 0 {
			return protocol.NullBulkReply
		}
	}

	var l list.List
	var key string
	var errReply redis.Reply
	keys := make([]string, 0)
	for i := 2; i < numkeys+2; i++ {
		l, errReply = d.getEntityAsList(string(args[i]))
		if errReply != nil {
			return errReply
		}
		if l != nil {
			key = string(args[i])
			break
		}
		keys = append(keys, string(args[i]))
	}

	if l != nil {
		writeKeys := []string{key}
		d.RWLocks(writeKeys, nil)
		defer d.RWUnLocks(writeKeys, nil)

		count = mathutil.Min(count, l.Length())

		values := make([][]byte, 0)
		for i := 0; i < count; i++ {
			if loc == "left" {
				values = append(values, l.RemoveFirst())
			} else {
				values = append(values, l.RemoveLast())
			}
		}
		if l.Length() == 0 {
			d.Remove(key)
		}

		res := protocol.NewArrayReply([]redis.Reply{protocol.NewBulkReply([]byte(key)), protocol.NewMultiBulkReply(values)})
		return res
	}

	// 如果所有key都不存在，则开始进行异步、阻塞逻辑
	kvChan := make(chan *KeyValues)
	go func(d *DB, kvChan chan *KeyValues) {
		timeTicker := time.Tick(time.Duration(timeout) * time.Second)
		idx := 0
		for {
			select {
			case <-timeTicker:
				kvChan <- nil
				return
			default:
				if idx == len(keys) {
					idx = 0
				}
				kvFunc := func() *KeyValues {
					key := keys[idx]
					d.RWLocks([]string{key}, nil)
					defer d.RWUnLocks([]string{key}, nil)

					l, _ := d.getEntityAsList(key)
					if l != nil {
						values := make([][]byte, 0)
						count := mathutil.Min(count, l.Length())
						for len(values) < count {
							if loc == "left" {
								values = append(values, l.RemoveFirst())
							} else {
								values = append(values, l.RemoveLast())
							}
						}
						if l.Length() == 0 {
							d.Remove(key)
						}
						return &KeyValues{key: []byte(key), values: values}
					}
					return nil
				}
				kv := kvFunc()
				if kv != nil {
					kvChan <- kv
					return
				}
				idx++

			}
		}
	}(d, kvChan)

	kv := <-kvChan
	if kv == nil {
		return protocol.NullBulkReply
	}
	return protocol.NewArrayReply([]redis.Reply{protocol.NewBulkReply(kv.key), protocol.NewMultiBulkReply(kv.values)})
}

func init() {
	registerNormalCommand("lpush", LPushCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("lpushx", LPushXCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpush", RPushCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpushx", RPushXCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("lpop", LPopCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("blpop", BLPopCommand, nil, -3, tagWrite)
	registerNormalCommand("rpop", RPopCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("brpop", BRPopCommand, nil, -3, tagWrite)
	registerNormalCommand("llen", LLenCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("lindex", LIndexCommand, readFirstKey, 3, tagRead)
	registerNormalCommand("lrange", LRangeCommand, readFirstKey, 4, tagRead)
	registerNormalCommand("linsert", LInsertCommand, writeFirstKey, 5, tagWrite)
	registerNormalCommand("lrem", LRemCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("lset", LSetCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("ltrim", LTrimCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("lmove", LMoveCommand, prepareLmove, 5, tagWrite)
	registerNormalCommand("blmove", BLMoveCommand, nil, 6, tagWrite)
	registerNormalCommand("lmpop", LMPopCommand, nil, -2, tagWrite)
	registerNormalCommand("blmpop", BLMPopCommand, nil, -5, tagWrite)

	// RPOPLPUSH, BRPOPLPUSH  已废弃
	// LPOS 有点麻烦，后续实现
}

// 根据列表长度，调整索引值，如果是负值，则转为正值
// 然后判断是否在 0 ~ length - 1 范围内
// 如果小于0，则置为0
// 如果大于length - 1，则返回错误
func adjustIndex(length, index int) (int, error) {
	if index < 0 {
		index += length
	}
	if index < 0 {
		return 0, nil
	}
	if index >= length {
		return 0, errors.New("index out of range")
	}
	return index, nil
}
