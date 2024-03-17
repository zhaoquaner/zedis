package database

import (
	"github.com/duke-git/lancet/v2/strutil"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"time"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/logger"
	"zedis/redis/protocol"
)

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx, set if not exists
	updatePolicy        // set xx, set if exists
)

var unlimitedTTL int64 = 0

func BuildStringEntity(val []byte) *db.DataEntity {
	return &db.DataEntity{
		Data: val,
		Type: db.StringType,
	}
}

// getEntityAsString 将value转换成string，如果不是string类型，返回错误
// 如果key不存在，则第一个返回值为nil
// 如果key存在，但不是string类型，返回err
func (d *DB) getEntityAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := d.GetEntity(key)
	if !ok {
		return nil, nil
	}
	if entity.Type != db.StringType {
		return nil, protocol.ErrorWrongTypeReply
	}
	return entity.Data.([]byte), nil
}

func SetCommand(d *DB, cmdArgs [][]byte) redis.Reply {
	key := string(cmdArgs[0])
	value := cmdArgs[1]

	policy := upsertPolicy
	ttl := unlimitedTTL

	if len(cmdArgs) > 2 {

		for i := 2; i < len(cmdArgs); i++ {
			arg := strings.ToUpper(string(cmdArgs[i]))
			switch arg {
			case "NX":
				if policy == updatePolicy {
					return protocol.ErrorSyntaxReply
				}
				policy = insertPolicy

			case "XX":
				if policy == insertPolicy {
					return protocol.ErrorSyntaxReply
				}
				policy = updatePolicy

			case "EX":
				if ttl != unlimitedTTL {
					// ttl 已经被设置过
					return protocol.ErrorSyntaxReply
				}
				if i+1 == len(cmdArgs) {
					return protocol.ErrorSyntaxReply
				}
				ttlArgs, err := parseTTL(cmdArgs[i+1], time.Second)
				if err != nil {
					return err
				}
				ttl = ttlArgs
				i++

			case "PX":
				if ttl != unlimitedTTL {
					// ttl 已经被设置过
					return protocol.ErrorSyntaxReply
				}
				if i+1 == len(cmdArgs) {
					return protocol.ErrorSyntaxReply
				}
				ttlArgs, err := parseTTL(cmdArgs[i+1], time.Millisecond)
				if err != nil {
					return err
				}
				ttl = ttlArgs
				i++
			case "EXAT":
				unixTimeArg, err := strconv.ParseInt(string(cmdArgs[i+1]), 10, 64)
				if err != nil {
					return protocol.NewErrorReply("ERR invalid expire time")
				}
				ttl = time.Unix(unixTimeArg, 0).Sub(time.Now()).Nanoseconds()
			case "PXAT":
				unixTimeArg, err := strconv.ParseInt(string(cmdArgs[i+1]), 10, 64)
				if err != nil {
					return protocol.NewErrorReply("ERR invalid expire time")
				}
				ttl = time.UnixMilli(unixTimeArg).Sub(time.Now()).Nanoseconds()
			default:
				return protocol.ErrorSyntaxReply
			}

		}

	}

	entity := BuildStringEntity(value)

	var result int

	switch policy {
	case upsertPolicy:
		d.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = d.PutIfAbsent(key, entity)
	case updatePolicy:
		result = d.PutIfExists(key, entity)
	}

	if result > 0 {
		if ttl != unlimitedTTL {
			logger.Infof("expire in second: %d", int(time.Duration(ttl).Seconds()))
			d.Expire(key, time.Duration(ttl))
		} else {
			d.Persist(key)
		}

		return protocol.OKReplyConst
	}

	return protocol.NullBulkReplyConst

}

func GetCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := d.getEntityAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return protocol.NullBulkReplyConst
	}
	return protocol.NewBulkReply(bytes)
}

// StrLenCommand strlen命令，返回字符串字节数，如果key不存在或类型错误，返回0
func StrLenCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := d.getEntityAsString(key)
	if err != nil || bytes == nil {
		return protocol.NewIntReply(0)
	}

	// 返回字节数，如果是中文，如: 测试，返回6，即一个utf8字符占3个字节
	return protocol.NewIntReply(int64(len(bytes)))
}

// AppendCommand append命令，向末尾追加字符串，如果key已存在且不是字符串类型，返回错误；如果key不存在，则创建key，返回字节长度
func AppendCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	valueBytes := args[1]
	bytes, err := d.getEntityAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		bytes = make([]byte, 0, len(valueBytes))
	}
	bytes = append(bytes, valueBytes...)
	d.PutEntity(key, BuildStringEntity(bytes))
	return protocol.NewIntReply(int64(len(bytes)))
}

// MSetCommand 设置多个key-value键值对，如果key已存在，则replace
func MSetCommand(d *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.NewArgNumErrReply("mset")
	}

	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val := args[i+1]
		d.PutEntity(string(key), BuildStringEntity(val))
	}
	return protocol.OKReplyConst
}

// MSetNxCommand 设置多个key-value键值对，只要有一个key已存在，就不执行任何操作，返回0;如果所有key都不存在，执行操作，返回1
func MSetNxCommand(d *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.NewArgNumErrReply("msetex")
	}
	kvMap := make(map[string][]byte)
	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val := args[i+1]
		kvMap[string(key)] = val
		_, ok := d.GetEntity(string(key))
		if ok {
			return protocol.ZeroReplyConst
		}
	}
	for k, v := range kvMap {
		d.PutEntity(k, BuildStringEntity(v))
	}
	return protocol.NewIntReply(1)
}

// MGetCommand 获取多个key的字符串value，数组类型，如果某个key不存在或类型不对，返回null
func MGetCommand(d *DB, args [][]byte) redis.Reply {
	res := make([]redis.Reply, 0, len(args))
	for _, arg := range args {
		valueBytes, err := d.getEntityAsString(string(arg))
		if err != nil || valueBytes == nil {
			res = append(res, protocol.NullBulkReplyConst)
		} else {
			res = append(res, protocol.NewBulkReply(valueBytes))
		}
	}
	return protocol.NewArrayReply(res)
}

// GetDelCommand 获取并删除key，如果key不存在或类型错误，返回nil
func GetDelCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := d.getEntityAsString(key)
	if err != nil || bytes == nil {
		return protocol.NullBulkReplyConst
	}

	d.Remove(key)
	return protocol.NewBulkReply(bytes)
}

// IncrCommand Incr给指定key的value加1，如果类型错误或无法解析为数值，返回错误;如果key不存在，则设置为0，再执行该操作
func IncrCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := d.getEntityAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		bytes = []byte("0")
	}
	parsedInt, err1 := strconv.ParseInt(string(bytes), 10, 64)
	if err1 != nil {
		return protocol.NewErrorReply("ERR value is not an integer or out of range")
	}
	parsedInt += 1
	d.PutEntity(key, BuildStringEntity([]byte(strconv.FormatInt(parsedInt, 10))))
	return protocol.NewIntReply(parsedInt)
}

// IncrByCommand IncrBy给指定key的value加指定数值，如果类型错误或无法解析为数值，返回错误;如果key不存在，则设置为0，再执行该操作
func IncrByCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	number, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	if err1 != nil {
		return protocol.NewErrorReply("ERR number is not an integer or out of range")
	}
	bytes, err := d.getEntityAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		bytes = []byte("0")
	}
	parsedInt, err2 := strconv.ParseInt(string(bytes), 10, 64)
	if err2 != nil {
		return protocol.NewErrorReply("ERR value is not an integer or out of range")
	}
	parsedInt += number
	d.PutEntity(key, BuildStringEntity([]byte(strconv.FormatInt(parsedInt, 10))))
	return protocol.NewIntReply(parsedInt)
}

// IncrByFloatCommand 给指定key的value加浮点数值
func IncrByFloatCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	floatArg, err := decimal.NewFromString(string(args[1]))
	if err != nil {
		return protocol.NewErrorReply("ERR value is not a valid float")
	}
	bytes, errReply := d.getEntityAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		bytes = []byte("0")
	}
	curNumber, err := decimal.NewFromString(string(bytes))
	if err != nil {
		return protocol.NewErrorReply("ERR value is not a valid float")
	}
	curNumber = decimal.Sum(floatArg, curNumber)
	curStr := []byte(curNumber.String())
	d.PutEntity(key, BuildStringEntity(curStr))
	return protocol.NewBulkReply(curStr)
}

// DecrCommand Decr给指定key的value减1，如果类型错误或无法解析为数值，返回错误；如果key不存在，则设置为0，再执行该操作
func DecrCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := d.getEntityAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		bytes = []byte("0")
	}
	parsedInt, err1 := strconv.ParseInt(string(bytes), 10, 64)
	if err1 != nil {
		return protocol.NewErrorReply("ERR value is not an integer or out of range")
	}
	parsedInt -= 1
	d.PutEntity(key, BuildStringEntity([]byte(strconv.FormatInt(parsedInt, 10))))
	return protocol.NewIntReply(parsedInt)
}

// DecrByCommand DecrBy给指定key的value减指定数值，如果类型错误或无法解析为数值，返回错误;如果key不存在，则设置为0，再执行该操作
func DecrByCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	number, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	if err1 != nil {
		return protocol.NewErrorReply("ERR number is not an integer or out of range")
	}
	bytes, err := d.getEntityAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		bytes = []byte("0")
	}
	parsedInt, err2 := strconv.ParseInt(string(bytes), 10, 64)
	if err2 != nil {
		return protocol.NewErrorReply("ERR value is not an integer or out of range")
	}
	parsedInt -= number
	d.PutEntity(key, BuildStringEntity([]byte(strconv.FormatInt(parsedInt, 10))))
	return protocol.NewIntReply(parsedInt)
}

func GetExCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if len(args) > 3 {
		return protocol.NewArgNumErrReply("getex")
	}

	bytes, errReply := d.getEntityAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return protocol.NullBulkReplyConst
	}

	if len(args) > 2 {
		ttl := unlimitedTTL
		arg := strings.ToUpper(string(args[1]))
		switch arg {
		case "EX":
			ttlArg, err := parseTTL(args[2], time.Second)
			if err != nil {
				return err
			}
			ttl = ttlArg
		case "PX":
			ttlArg, err := parseTTL(args[2], time.Millisecond)
			if err != nil {
				return err
			}
			ttl = ttlArg
		case "EXAT":
			unixTimeArg, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err != nil {
				return protocol.NewErrorReply("ERR invalid expire time")
			}
			ttl = time.Unix(unixTimeArg, 0).Sub(time.Now()).Nanoseconds()
		case "PXAT":
			unixTimeArg, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err != nil {
				return protocol.NewErrorReply("ERR invalid expire time")
			}
			ttl = time.UnixMilli(unixTimeArg).Sub(time.Now()).Nanoseconds()
		default:
			return protocol.ErrorSyntaxReply
		}

		if ttl != unlimitedTTL {
			d.Expire(key, time.Duration(ttl))
		}

	} else if len(args) == 2 {
		arg := strings.ToUpper(string(args[1]))
		if arg != "PERSIST" {
			return protocol.ErrorSyntaxReply
		}
		d.Persist(key)
	}

	return protocol.NewBulkReply(bytes)
}

// SetRangeCommand 设置字符串子串，指定offset参数，表示从offset位置开始替换，如果offset大于字符串长度，则补零至offset长度，再执行替换操作
// 如果要替换的字符串长度加上offset大于原字符串长度，则加长并替换
// 如果key不存在，则视为空字符串
func SetRangeCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offsetArg := args[1]
	val := args[2]
	offset, err := parseInt64(offsetArg)
	if err != nil {
		return protocol.NewErrorReply("ERR value is not an integer or out of range")
	}
	bytes, errReply := d.getEntityAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		bytes = make([]byte, 0)
	}

	bytesLen := int64(len(bytes))
	if bytesLen < offset {
		bytes = append(bytes, make([]byte, offset-bytesLen)...)
		bytesLen = int64(len(bytes))
	}

	for i := 0; i < len(val); i++ {
		idx := offset + int64(i)
		if idx >= bytesLen {
			bytes = append(bytes, val[i])
		} else {
			bytes[idx] = val[i]
		}
	}

	d.PutEntity(key, BuildStringEntity(bytes))
	return protocol.NewIntReply(int64(len(bytes)))
}

// GetRangeCommand 获取字符串子串，指定start和end，如果为负数，表示从末尾往前数，-1表示最后一个字符
func GetRangeCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, errReply := d.getEntityAsString(key)
	if errReply != nil {
		return protocol.ErrorWrongTypeReply
	}
	if bytes == nil {
		return protocol.ErrorKeyNotExistsReply
	}
	startArg, err := parseInt64(args[1])
	endArg, err := parseInt64(args[2])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}

	strlen := int64(len(bytes))
	start := startArg
	end := endArg
	if start > 0 {
		if start > strlen {
			start = strlen
		}
	} else if start < 0 {
		if -start > strlen {
			start = 0
		} else {
			start = strlen + start
		}
	}

	if end > 0 {
		if end > strlen {
			end = strlen
		}
	} else if end < 0 {
		if -end > strlen {
			end = 0
		} else {
			end = strlen + end
		}
	}
	if start > end {
		return protocol.EmptyBulkReplyConst
	}
	return protocol.NewBulkReply([]byte(strutil.Substring(string(bytes), int(start), uint(end-start+1))))
}

func init() {
	registerNormalCommand("set", SetCommand, WriteFirstKey, -3, tagWrite)
	registerNormalCommand("get", GetCommand, ReadFirstKey, 2, tagRead)
	registerNormalCommand("strlen", StrLenCommand, ReadFirstKey, 2, tagRead)
	registerNormalCommand("append", AppendCommand, ReadFirstKey, 3, tagWrite)
	registerNormalCommand("mset", MSetCommand, WriteAllKeys, -3, tagWrite)
	registerNormalCommand("msetnx", MSetNxCommand, WriteAllKeys, -3, tagWrite)
	registerNormalCommand("mget", MGetCommand, ReadAllKeys, -2, tagRead)
	registerNormalCommand("getdel", GetDelCommand, WriteFirstKey, 2, tagWrite)
	registerNormalCommand("incr", IncrCommand, WriteFirstKey, 2, tagWrite)
	registerNormalCommand("decr", DecrCommand, WriteFirstKey, 2, tagWrite)
	registerNormalCommand("incrby", IncrByCommand, WriteFirstKey, 3, tagWrite)
	registerNormalCommand("decrby", DecrByCommand, WriteFirstKey, 3, tagWrite)
	registerNormalCommand("getex", GetExCommand, ReadFirstKey, -2, tagRead)
	registerNormalCommand("setrange", SetRangeCommand, WriteFirstKey, 4, tagWrite)
	registerNormalCommand("getrange", GetRangeCommand, ReadFirstKey, 4, tagRead)
	registerNormalCommand("incrbyfloat", IncrByFloatCommand, WriteFirstKey, 3, tagWrite)

	// GETSET Deprecated
	// LCS 不会
	// PSETEX Deprecated
	// SETEX Deprecated
	// SETNX Deprecated
	// SUBLEN Deprecated
}
