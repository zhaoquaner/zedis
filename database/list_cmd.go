package database

import (
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

func init() {
	registerNormalCommand("lpush", LPushCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("lpushx", LPushXCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpush", RPushCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("lpop", LPopCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("rpop", RPopCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("llen", LLenCommand, readFirstKey, 2, tagRead)
}
