package database

import (
	"github.com/duke-git/lancet/v2/mathutil"
	"strings"
	"zedis/datastruct/list"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/redis/protocol"
)

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
		l = list.NewLinkedList(args[1:])
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
		return protocol.ZeroReplyConst
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
		l = list.NewLinkedList(args[1:])
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
		return protocol.ZeroReplyConst
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
		return protocol.NullBulkReplyConst
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
		return protocol.NullBulkReplyConst
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

// LLenCommand 返回list长度
// LLEN key
func LLenCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	l, errReply := d.getEntityAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.ZeroReplyConst
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
		return protocol.NullBulkReplyConst
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
		return protocol.EmptyMultiBulkReplyConst
	}
	length := l.Length()

	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}

	if end < 0 {
		end = length + end
		if end < 0 {
			end = 0
		}
	}

	end = mathutil.Min(length-1, end)
	if start > length-1 || start > end {
		return protocol.EmptyMultiBulkReplyConst
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
		return protocol.ZeroReplyConst
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

func init() {
	registerNormalCommand("lpush", LPushCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("lpushx", LPushXCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpush", RPushCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpushx", RPushXCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("lpop", LPopCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpop", RPopCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("llen", LLenCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("lindex", LIndexCommand, readFirstKey, 3, tagRead)
	registerNormalCommand("lrange", LRangeCommand, readFirstKey, 4, tagRead)
	registerNormalCommand("linsert", LInsertCommand, writeFirstKey, 5, tagWrite)
}
