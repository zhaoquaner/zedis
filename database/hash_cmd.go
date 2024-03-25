package database

import (
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"zedis/datastruct/dict"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/redis/protocol"
)

func (d *DB) getEntityAsHash(key string) (dict.Dict, redis.Reply) {
	entity, exists := d.GetEntity(key)
	if !exists {
		return nil, nil
	}
	if entity.Type != db.HashType {
		return nil, protocol.ErrorWrongTypeReply
	}
	return entity.Data.(dict.Dict), nil
}

func buildHashEntity(hash dict.Dict) *db.DataEntity {
	return &db.DataEntity{
		Data: hash,
		Type: db.HashType,
	}
}

// HSetCommand 向hash中添加元素，如果hash中已存在某个field，则覆盖; 返回新建(不包括被覆盖的)元素的数量
// HSET key field value [field value ...]
func HSetCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if len(args)%2 != 1 {
		return protocol.NewArgNumErrReply("HSET")
	}

	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}

	keyExists := hash != nil
	if !keyExists {
		hash = dict.NewSimpleDict()
	}

	insertedCount := 0

	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		value := args[i+1]
		insertedCount += hash.Put(field, value)
	}
	if !keyExists {
		d.PutEntity(key, buildHashEntity(hash))
	}
	return protocol.NewIntReply(int64(insertedCount))
}

// HSetNXCommand 向hash中添加元素，如果hash中已存在某个field，则不进行操作
// HSETNX key field value
func HSetNXCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}

	keyExists := hash != nil
	if !keyExists {
		hash = dict.NewSimpleDict()
		d.PutEntity(key, buildHashEntity(hash))
	}

	if hash.Exists(field) {
		return protocol.ZeroReply
	}
	hash.Put(field, args[2])
	return protocol.NewIntReply(1)
}

// HGetCommand 返回hash中指定field对应的value
// HGET key field
func HGetCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.NullBulkReply
	}
	v, exists := hash.Get(field)
	if !exists {
		return protocol.NullBulkReply
	}
	return protocol.NewBulkReply(v.([]byte))
}

// HGetAllCommand 获取hash所有key-value键值对；返回格式为数组，一个key后跟对应value；所以数组长度是hash键值对个数的二倍
// HGETALL key
func HGetAllCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.EmptyMultiBulkReply
	}
	res := make([][]byte, 0)
	hash.ForEach(func(key string, val any) bool {
		res = append(res, []byte(key), val.([]byte))
		return true
	})
	return protocol.NewMultiBulkReply(res)
}

// HExistsCommand 返回hash中是否存在某个field；不存在返回0，存在返回1
// HEXISTS key field
func HExistsCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.ZeroReply
	}
	if hash.Exists(field) {
		return protocol.NewIntReply(1)
	} else {
		return protocol.ZeroReply
	}
}

// HLenCommand 获取hash键值对个数
// HLEN key
func HLenCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.ZeroReply
	}
	return protocol.NewIntReply(int64(hash.Len()))
}

// HKeysCommand 以数组形式返回hash中所有key
// HKEYS key
func HKeysCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.EmptyMultiBulkReply
	}
	keys := make([][]byte, 0)
	hash.ForEach(func(key string, val any) bool {
		keys = append(keys, []byte(key))
		return true
	})
	return protocol.NewMultiBulkReply(keys)
}

// HDelCommand 删除hash中多个field，如果field不存在，则忽略，返回实际删除的field数量
// HDEL key field [field ...]
// 删除完成后，如果hash中没有元素了，则删除该hash
func HDelCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.ZeroReply
	}
	deletedCount := 0
	for i := 1; i < len(args); i++ {
		field := string(args[i])
		_, res := hash.Remove(field)
		deletedCount += res
	}
	if hash.Len() == 0 {
		d.Remove(key)
	}
	return protocol.NewIntReply(int64(deletedCount))
}

// HIncrByCommand hash中指定的field解析为int类型，并加increment，如果key不存在，则新建hash，并将field对应value置为0；如果field不存在，则同样视为0
// 返回增加后的值
// HINCRBY key field increment
func HIncrByCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	increment, err := parseInt(args[2])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		hash = dict.NewSimpleDict()
		hash.Put(field, []byte(strconv.Itoa(increment)))
		d.PutEntity(key, buildHashEntity(hash))
		return protocol.NewIntReply(int64(increment))
	}
	var oldValue int
	val, exists := hash.Get(field)
	if !exists {
		oldValue = 0
	} else {
		oldValue, err = parseInt(val.([]byte))
		if err != nil {
			return protocol.NewErrorReply("ERR hash value is not an integer")
		}
	}
	oldValue += increment
	hash.Put(field, []byte(strconv.Itoa(oldValue)))
	return protocol.NewIntReply(int64(oldValue))
}

// HIncrByFloatCommand hash中指定的field解析为float类型，并加increment，如果key不存在，则新建hash，并将field对应value置为0；如果field不存在，则同样视为0
// 返回增加后的值
// HINCRBYFLOAT key field increment
func HIncrByFloatCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	increment, err := decimal.NewFromString(string(args[2]))
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		hash = dict.NewSimpleDict()
		hash.Put(field, []byte(increment.String()))
		d.PutEntity(key, buildHashEntity(hash))
		return protocol.NewBulkReply([]byte(increment.String()))
	}
	var oldValue decimal.Decimal
	val, exists := hash.Get(field)
	if !exists {
		oldValue = decimal.NewFromInt(0)
	} else {
		oldValue, err = decimal.NewFromString(string(val.([]byte)))
		if err != nil {
			return protocol.NewErrorReply("ERR hash value is not a float")
		}
	}
	oldValue = oldValue.Add(increment)
	hash.Put(field, []byte(oldValue.String()))
	return protocol.NewBulkReply([]byte(oldValue.String()))
}

// HMGetCommand 获取hash中多个field对应的value，对于每一个field，如果不存在，则vlaue返回nil；因此返回数据个数为数组，数组顺序、长度与传入的field顺序、个数保持一致
// HMGET key field [field ...]
// 如果key不存在，则返回一个nil数组
func HMGetCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := make([]redis.Reply, len(args)-1)
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	for i := 1; i < len(args); i++ {
		field := string(args[i])
		if hash == nil || !hash.Exists(field) {
			values[i-1] = protocol.NullBulkReply
		} else {
			val, _ := hash.Get(field)
			values[i-1] = protocol.NewBulkReply(val.([]byte))
		}
	}
	return protocol.NewArrayReply(values)
}

// HStrLenCommand 返回hash中field对应value的字节长度；如果key不存在，或field不存在，返回0
// HSTRLEN key field
func HStrLenCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil || !hash.Exists(field) {
		return protocol.ZeroReply
	}
	val, _ := hash.Get(field)
	return protocol.NewIntReply(int64(len(val.([]byte))))
}

// HValsCommand 返回hash中所有value，以数组形式返回
// HVALS key
func HValsCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.EmptyMultiBulkReply
	}
	vals := make([][]byte, 0)
	hash.ForEach(func(key string, val any) bool {
		vals = append(vals, val.([]byte))
		return true
	})
	return protocol.NewMultiBulkReply(vals)
}

// HRandFieldCommand 返回hash中随机的多个field(和value)
// 如果不指定count，则默认只返回1个；如果指定了count，根据正负分为：
// 1. count > 0, 返回的多个不重复的field，返回的个数是count和hash长度的较小值
// 2. count < 0, 返回count个可以重复的field
// 3. count = 0, 返回空数组
// 如果指定了withvalues参数，则返回的格式变为，一个field带着一个value，即数组长度变为原来二倍
// 注意：只有指定了count参数，才能指定withvalues参数
// 如果只返回一个field，则reply类型为bulk string
// 如果 key不存在，返回Nil
//
// HRANDFIELD key [count [withvalues]]
func HRandFieldCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 3 {
		return protocol.NewArgNumErrReply("HRANDFIELD")
	}
	key := string(args[0])
	count := 1
	var err error
	if len(args) >= 2 {
		count, err = parseInt(args[1])
		if err != nil {
			return protocol.NewErrorReply("ERR value is not an integer or out of range")
		}
	}
	if count == 0 {
		return protocol.EmptyMultiBulkReply
	}
	withValues := false
	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withValues = true
		} else {
			return protocol.ErrorSyntaxReply
		}
	}

	hash, errReply := d.getEntityAsHash(key)
	if errReply != nil {
		return errReply
	}
	if hash == nil {
		return protocol.EmptyMultiBulkReply
	}

	var fields []string
	if count > 0 {
		fields = hash.RandomDistinctKeys(count)
	} else {
		fields = hash.RandomKeys(-count)
	}
	res := make([][]byte, 0)
	for _, field := range fields {
		res = append(res, []byte(field))
		if withValues {
			val, _ := hash.Get(field)
			res = append(res, val.([]byte))
		}
	}
	return protocol.NewMultiBulkReply(res)
}

func init() {
	registerNormalCommand("hset", HSetCommand, writeFirstKey, -4, tagWrite)
	registerNormalCommand("hsetnx", HSetNXCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("hget", HGetCommand, readFirstKey, 3, tagRead)
	registerNormalCommand("hgetall", HGetAllCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("hexists", HExistsCommand, readFirstKey, 3, tagRead)
	registerNormalCommand("hlen", HLenCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("hkeys", HKeysCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("hdel", HDelCommand, writeFirstKey, -3, tagWrite)

	registerNormalCommand("hincrby", HIncrByCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("hincrbyfloat", HIncrByFloatCommand, writeFirstKey, 4, tagWrite)

	registerNormalCommand("hmget", HMGetCommand, readFirstKey, -3, tagRead)
	registerNormalCommand("hstrlen", HStrLenCommand, readFirstKey, 3, tagRead)
	registerNormalCommand("hvals", HValsCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("hrandfield", HRandFieldCommand, readFirstKey, -2, tagRead)

	// HScan有点复杂，暂不实现
	// HMSET 已废弃，HSET可实现相同功能
}
