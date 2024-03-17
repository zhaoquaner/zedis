package database

import (
	"time"
	"zedis/interface/redis"
	"zedis/lib/wildcard"
	"zedis/redis/protocol"
)

var (
	defaultExpirePolicy   = "defaultExpirePolicy"   // 默认的设置过期时间策略，无论存不存在，大小，一律设置
	insertExpirePolicy    = "insertExpirePolicy"    // 只有不存在过期时间，才插入
	updateExpirePolicy    = "updateExpirePolicy"    // 只有存在过期时间，才更新
	greatThanExpirePolicy = "greatThanExpirePolicy" // 只有新过期时间大于原过期时间，才更新
	lessThanExpirePolicy  = "lessThanExpirePolicy"  // 只有新过期时间小于原过期时间，才更新
)

// ExistsCommand 查询key是否存在，返回key存在的数量
func ExistsCommand(d *DB, args [][]byte) redis.Reply {
	var existCount int64 = 0
	for i := 0; i < len(args); i++ {
		if d.Exists(string(args[i])) {
			existCount++
		}
	}
	return protocol.NewIntReply(existCount)
}

// DelCommand 删除所有key对应键值对，返回删除成功的数量
func DelCommand(d *DB, args [][]byte) redis.Reply {
	keys := make([]string, 0)
	for i := 0; i < len(args); i++ {
		keys = append(keys, string(args[i]))
	}
	return protocol.NewIntReply(int64(d.Removes(keys...)))
}

// KeysCommand 返回pattern对应的所有key，pattern为通配符
func KeysCommand(d *DB, args [][]byte) redis.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	keys := make([][]byte, 0)
	if err != nil {
		return protocol.NewErrorReply("ERR pattern is not a valid regex expression")
	}
	d.ForEach(func(key string, val any) bool {
		if pattern.IsMatch(key) && !d.IsExpired(key) {
			keys = append(keys, []byte(key))
		}
		return true
	})
	return protocol.NewMultiBulkReply(keys)
}

// ExpireCommand 给key设置过期时间，传入的参数为秒数，同时根据NX、XX、GT、LT不同选项，执行相应检验
func ExpireCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 3 {
		return protocol.NewArgNumErrReply("expire")
	}
	key := string(args[0])

	expirePolicy := defaultExpirePolicy
	if len(args) == 3 {
		expirePolicy = getExpirePolicy(string(args[2]))
	}

	oldExpireTimeVal, exists := d.ttlMap.Get(key)
	var oldExpireTime time.Time
	if exists {
		oldExpireTime = oldExpireTimeVal.(time.Time)
	}

	ttl, errReply := parseTTL(args[1], time.Second)
	if errReply != nil {
		return errReply
	}

	newExpireTime := time.Now().Add(time.Duration(ttl))
	return setExpireTime(d, key, expirePolicy, exists, oldExpireTime, newExpireTime)
}

// ExpireAtCommand 给key设置过期时间，传入的参数为Unix timestamp(精确到秒)，同时根据NX、XX、GT、LT不同选项，执行相应检验
func ExpireAtCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 3 {
		return protocol.NewArgNumErrReply("expire")
	}
	key := string(args[0])
	expirePolicy := defaultExpirePolicy
	if len(args) == 3 {
		expirePolicy = getExpirePolicy(string(args[2]))
	}

	oldExpireTimeVal, exists := d.ttlMap.Get(key)
	var oldExpireTime time.Time
	if exists {
		oldExpireTime = oldExpireTimeVal.(time.Time)
	}

	timestamp, err := parseInt64(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}

	newExpireTime := time.Unix(timestamp, 0)
	return setExpireTime(d, key, expirePolicy, exists, oldExpireTime, newExpireTime)
}

// PExpireCommand 给key设置过期时间，传入的参数为毫秒数，同时根据NX、XX、GT、LT不同选项，执行相应检验
func PExpireCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 3 {
		return protocol.NewArgNumErrReply("expire")
	}
	key := string(args[0])

	expirePolicy := defaultExpirePolicy
	if len(args) == 3 {
		expirePolicy = getExpirePolicy(string(args[2]))
	}

	oldExpireTimeVal, exists := d.ttlMap.Get(key)
	var oldExpireTime time.Time
	if exists {
		oldExpireTime = oldExpireTimeVal.(time.Time)
	}

	ttl, errReply := parseTTL(args[1], time.Millisecond)
	if errReply != nil {
		return errReply
	}

	newExpireTime := time.Now().Add(time.Duration(ttl))
	return setExpireTime(d, key, expirePolicy, exists, oldExpireTime, newExpireTime)
}

func PExpireAtCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 3 {
		return protocol.NewArgNumErrReply("expire")
	}
	key := string(args[0])
	expirePolicy := defaultExpirePolicy
	if len(args) == 3 {
		expirePolicy = getExpirePolicy(string(args[2]))
	}

	oldExpireTimeVal, exists := d.ttlMap.Get(key)
	var oldExpireTime time.Time
	if exists {
		oldExpireTime = oldExpireTimeVal.(time.Time)
	}

	timestamp, err := parseInt64(args[1])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}

	newExpireTime := time.UnixMilli(timestamp)
	return setExpireTime(d, key, expirePolicy, exists, oldExpireTime, newExpireTime)
}

// getExpirePolicy 根据传入的参数arg，返回指定的过期时间设置策略
func getExpirePolicy(arg string) string {
	switch arg {
	case "NX":
		return insertExpirePolicy
	case "XX":
		return updateExpirePolicy
	case "GT":
		return greatThanExpirePolicy
	case "LT":
		return lessThanExpirePolicy
	default:
		return defaultExpirePolicy
	}
}

// 根据新旧过期时间和过期时间设置策略，设置key的过期时间，并返回响应
func setExpireTime(d *DB, key, expirePolicy string, oldExists bool, oldExpireTime, newExpireTime time.Time) redis.Reply {
	switch expirePolicy {
	case defaultExpirePolicy:
		d.ExpireByTime(key, newExpireTime)
		return protocol.NewIntReply(1)
	case insertExpirePolicy:
		if oldExists {
			return protocol.ZeroReplyConst
		}
	case updateExpirePolicy:
		if !oldExists {
			return protocol.ZeroReplyConst
		}
	case greatThanExpirePolicy:
		if !oldExists || !newExpireTime.After(oldExpireTime) {
			return protocol.ZeroReplyConst
		}
	case lessThanExpirePolicy:
		if !oldExists || !newExpireTime.Before(oldExpireTime) {
			return protocol.ZeroReplyConst
		}

	}
	d.ExpireByTime(key, newExpireTime)
	return protocol.NewIntReply(1)
}

// ExpireTimeCommand 返回key的过期时间，timestamp(精确到秒)格式，如果key存在但没有设置过期时间，返回-1，如果key不存在返回-2
func ExpireTimeCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if !d.Exists(key) {
		return protocol.NewIntReply(-1)
	}
	expireTimeVal, exists := d.ttlMap.Get(key)
	if !exists {
		return protocol.NewIntReply(-2)
	}
	expireTime := expireTimeVal.(time.Time)
	return protocol.NewIntReply(expireTime.Unix())
}

// PExpireTimeCommand 返回key的过期时间，timestamp(精确到毫秒)格式，如果key存在但没有设置过期时间，返回-1，如果key不存在返回-2
func PExpireTimeCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if !d.Exists(key) {
		return protocol.NewIntReply(-1)
	}
	expireTimeVal, exists := d.ttlMap.Get(key)
	if !exists {
		return protocol.NewIntReply(-2)
	}
	expireTime := expireTimeVal.(time.Time)
	return protocol.NewIntReply(expireTime.UnixMilli())
}

// TTLCommand 返回key 剩多少秒过期，如果key存在但没有设置过期时间，返回-1，如果key不存在返回-2
func TTLCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if !d.Exists(key) {
		return protocol.NewIntReply(-1)
	}
	expireTimeVal, exists := d.ttlMap.Get(key)
	if !exists {
		return protocol.NewIntReply(-2)
	}
	expireTime := expireTimeVal.(time.Time)
	return protocol.NewIntReply(int64(expireTime.Sub(time.Now()).Seconds()))
}

// PTTLCommand 返回key 剩多少毫秒过期，如果key存在但没有设置过期时间，返回-1，如果key不存在返回-2
func PTTLCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if !d.Exists(key) {
		return protocol.NewIntReply(-1)
	}
	expireTimeVal, exists := d.ttlMap.Get(key)
	if !exists {
		return protocol.NewIntReply(-2)
	}
	expireTime := expireTimeVal.(time.Time)
	return protocol.NewIntReply(expireTime.Sub(time.Now()).Milliseconds())
}

func init() {
	registerNormalCommand("exists", ExistsCommand, ReadAllKeys, -2, tagRead)
	registerNormalCommand("del", DelCommand, WriteAllKeys, -2, tagWrite)
	registerNormalCommand("keys", KeysCommand, noPrepare, 2, tagRead)
	registerNormalCommand("expire", ExpireCommand, WriteFirstKey, -3, tagWrite)
	registerNormalCommand("expireat", ExpireAtCommand, WriteFirstKey, -3, tagWrite)
	registerNormalCommand("pexpire", PExpireCommand, WriteFirstKey, -3, tagWrite)
	registerNormalCommand("pexpireat", PExpireAtCommand, WriteFirstKey, -3, tagWrite)
	registerNormalCommand("expiretime", ExpireTimeCommand, ReadFirstKey, 2, tagRead)
	registerNormalCommand("pexpiretime", PExpireTimeCommand, ReadFirstKey, 2, tagRead)
	registerNormalCommand("ttl", TTLCommand, ReadFirstKey, 2, tagRead)
	registerNormalCommand("pttl", PTTLCommand, ReadFirstKey, 2, tagRead)
}