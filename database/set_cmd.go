package database

import (
	"github.com/duke-git/lancet/v2/mathutil"
	"strings"
	setds "zedis/datastruct/set"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/redis/protocol"
)

func (d *DB) getEntityAsSet(key string) (setds.Set, redis.Reply) {
	entity, exists := d.GetEntity(key)
	if !exists {
		return nil, nil
	}
	if entity.Type != db.SetType {
		return nil, protocol.ErrorWrongTypeReply
	}
	return entity.Data.(setds.Set), nil
}

func buildSetEntity(set setds.Set) *db.DataEntity {
	return &db.DataEntity{
		Data: set,
		Type: db.SetType,
	}
}

// SAddCommand 向集合添加元素，如果key不存在，创建集合；如果key为其他类型，返回错误
// 返回成功添加的元素数
func SAddCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	keyExists := set != nil
	if !keyExists {
		set = setds.NewSet()
	}

	var count = 0
	for i := 1; i < len(args); i++ {
		count += set.Add(string(args[i]))
	}
	if !keyExists {
		d.PutEntity(key, buildSetEntity(set))
	}
	return protocol.NewIntReply(int64(count))
}

// SMembersCommand 返回集合中所有元素； key不存在，返回空数组；key为其他类型，返回错误
func SMembersCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.EmptyMultiBulkReply
	}

	members := make([][]byte, set.Len())
	idx := 0
	for _, member := range set.Members() {
		members[idx] = []byte(member)
		idx++
	}
	return protocol.NewMultiBulkReply(members)
}

// SCardCommand 返回集合元素的数量，key不存在返回0；类型不对返回错误
func SCardCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.ZeroReply
	}
	return protocol.NewIntReply(int64(set.Len()))
}

// SRemCommand 删除指定的members，如果删除后set空了，删除该set；key不存在，返回0；类型不对，返回错误
// 返回删除成功的member
func SRemCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.ZeroReply
	}
	deletedCount := 0
	for i := 1; i < len(args); i++ {
		deletedCount += set.Remove(string(args[i]))
	}
	if set.Len() == 0 {
		d.Remove(key)
	}
	return protocol.NewIntReply(int64(deletedCount))
}

// getSetByArgs 根据传入的args求key对应的集合差集
func getSetsByArgs(d *DB, args [][]byte) ([]setds.Set, redis.Reply) {
	keys := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		keys[i] = string(args[i])
	}

	sets := make([]setds.Set, len(keys))
	idx := 0
	for _, key := range keys {
		set, errReply := d.getEntityAsSet(key)
		if errReply != nil {
			return nil, errReply
		}
		if set == nil {
			set = setds.NewSet()
		}
		sets[idx] = set
		idx++
	}
	return sets, nil
}

// SDiffCommand 第一个key集合与后续多个指定的key集合取差集，如果某个key不存在，认为是空集合
// 如果只有一个key，返回该集合的所有元素
func SDiffCommand(d *DB, args [][]byte) redis.Reply {
	sets, errReply := getSetsByArgs(d, args)
	if errReply != nil {
		return errReply
	}
	diffSet := setds.Diff(sets...)
	ret := make([][]byte, diffSet.Len())
	idx := 0
	for _, member := range diffSet.Members() {
		ret[idx] = []byte(member)
		idx++
	}
	return protocol.NewMultiBulkReply(ret)
}

// SDiffStoreCommand 与sdiff命令类似，但是会将取差集后的元素存入到新key中，并返回差集元素的个数
// sdiffstore newKey key1 [key2 ...]
func SDiffStoreCommand(d *DB, args [][]byte) redis.Reply {
	newKey := string(args[0])
	sets, errReply := getSetsByArgs(d, args[1:])
	if errReply != nil {
		return errReply
	}
	diffSet := setds.Diff(sets...)

	ret := d.PutEntity(newKey, buildSetEntity(diffSet))
	if ret == 0 {
		d.Persist(newKey)
	}
	return protocol.NewIntReply(int64(diffSet.Len()))
}

func SUnionCommand(d *DB, args [][]byte) redis.Reply {
	sets, errReply := getSetsByArgs(d, args)
	if errReply != nil {
		return errReply
	}
	diffSet := setds.Union(sets...)
	ret := make([][]byte, diffSet.Len())
	idx := 0
	for _, member := range diffSet.Members() {
		ret[idx] = []byte(member)
		idx++
	}
	return protocol.NewMultiBulkReply(ret)
}

func SUnionStoreCommand(d *DB, args [][]byte) redis.Reply {
	newKey := string(args[0])
	sets, errReply := getSetsByArgs(d, args[1:])
	if errReply != nil {
		return errReply
	}
	diffSet := setds.Union(sets...)

	ret := d.PutEntity(newKey, buildSetEntity(diffSet))
	if ret == 0 {
		d.Persist(newKey)
	}
	return protocol.NewIntReply(int64(diffSet.Len()))
}

// SInterCommand 交集并返回
func SInterCommand(d *DB, args [][]byte) redis.Reply {
	sets, errReply := getSetsByArgs(d, args)
	if errReply != nil {
		return errReply
	}
	diffSet := setds.Intersect(sets...)
	ret := make([][]byte, diffSet.Len())
	idx := 0
	for _, member := range diffSet.Members() {
		ret[idx] = []byte(member)
		idx++
	}
	return protocol.NewMultiBulkReply(ret)
}

// SInterStoreCommand 多个key对应集合取交集，并存储到newKey上
// 格式：SINTERSTORE destination key [key ...]
func SInterStoreCommand(d *DB, args [][]byte) redis.Reply {
	newKey := string(args[0])
	sets, errReply := getSetsByArgs(d, args[1:])
	if errReply != nil {
		return errReply
	}
	diffSet := setds.Intersect(sets...)

	ret := d.PutEntity(newKey, buildSetEntity(diffSet))
	if ret == 0 {
		d.Persist(newKey)
	}
	return protocol.NewIntReply(int64(diffSet.Len()))
}

// SInterCardCommand 返回多个key对应集合的交集元素个数
// 格式：SINTERCARD numkeys key [key ...] [LIMIT limit]
// numkeys表示后面有几个key
// limit参数可选，表示如果交集元素个数大于等于limit，就直接返回limit，否则返回实际交集元素个数，默认为0，表示不限制
func SInterCardCommand(d *DB, args [][]byte) redis.Reply {
	var err error
	var errReply redis.Reply
	numKeys, err := parseInt(args[0])
	if err != nil {
		return protocol.ErrorSyntaxReply
	}
	limit := 0
	var sets []setds.Set

	if len(args) == numKeys+1 {
		sets, errReply = getSetsByArgs(d, args[1:])
		if errReply != nil {
			return errReply
		}
	} else if len(args) == numKeys+3 {
		if strings.ToUpper(string(args[len(args)-2])) != "LIMIT" {
			return protocol.ErrorSyntaxReply
		}
		var err error
		limit, err = parseInt(args[len(args)-1])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
		sets, errReply = getSetsByArgs(d, args[1:len(args)-2])
		if errReply != nil {
			return errReply
		}
	} else {
		return protocol.ErrorSyntaxReply
	}

	interSet := setds.Intersect(sets...)
	if limit == 0 {
		return protocol.NewIntReply(int64(interSet.Len()))
	} else {
		return protocol.NewIntReply(int64(mathutil.Min(interSet.Len(), limit)))
	}
}

// SIsMemberCommand 查询某个元素是否是key对应集合元素
// 格式：SISMEMBER key member
// key不存在或member不在集合中返回 0
// 类型不对返回错误
// member在集合中 返回1
func SIsMemberCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.ZeroReply
	}
	var ret int64 = 0
	if set.Contains(string(args[1])) {
		ret = 1
	}
	return protocol.NewIntReply(ret)
}

// SMIsMemberCommand 返回多个member是否在key对应集合中，返回类型为数组，与member顺序一一对应，存在为1，不存在为0
// 格式 SMISMEMBER key member [member ...]
// 类型不对返回错误
func SMIsMemberCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	ret := make([]redis.Reply, len(args)-1)
	idx := 0
	for _, member := range args[1:] {
		var contain int64 = 0
		if set != nil && set.Contains(string(member)) {
			contain = 1
		}
		ret[idx] = protocol.NewIntReply(contain)
		idx++
	}
	return protocol.NewArrayReply(ret)
}

// SMoveCommand 将指定的member从source key集合移到destination key集合，移动成功，返回1，否则返回0
// 格式：SMOVE source destination member
// 如果source key不存在，或者source 集合中不存在member，返回0
// 如果destination不存在，则创建
// 如果destination 集合中已存在member，则仅从source中删除member，返回0
// 如果source和destination 类型都不是集合，返回错误
func SMoveCommand(d *DB, args [][]byte) redis.Reply {
	source := string(args[0])
	dest := string(args[1])
	member := string(args[2])

	sourceSet, errReply := d.getEntityAsSet(source)
	if errReply != nil {
		return errReply
	}
	destSet, errReply := d.getEntityAsSet(dest)
	if errReply != nil {
		return errReply
	}

	if sourceSet == nil || !sourceSet.Contains(member) {
		return protocol.ZeroReply
	}

	sourceSet.Remove(member)
	if sourceSet.Len() == 0 {
		d.Remove(source)
	}
	if destSet == nil {
		destSet = setds.NewSet(member)
		d.PutEntity(dest, buildSetEntity(destSet))
	} else {
		destSet.Add(member)
	}
	return protocol.NewIntReply(1)
}

// SRandMemberCommand 随机返回count个集合中元素，如果不存在count参数，则默认返回1个
// 格式: SRANDMEMBER key [count]
// 如果key不存在，返回Nil reply
// 如果只返回1个，类型为Bulk string
// 如果返回多个，类型为MultiBulk string
func SRandMemberCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 2 {
		return protocol.NewArgNumErrReply("SRANDMEMBER")
	}

	key := string(args[0])
	count := 1
	if len(args) == 2 {
		var err error
		count, err = parseInt(args[1])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
	}

	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.NullBulkReply
	}
	members := set.RandomDistinctMembers(count)
	memberBytes := make([][]byte, len(members))
	idx := 0
	for _, member := range members {
		memberBytes[idx] = []byte(member)
		idx++
	}
	return protocol.NewMultiBulkReply(memberBytes)
}

// SPopCommand 随机返回count个集合中元素，并删除这些元素，如果不存在count参数，则默认返回并删除1个
// 格式: SPOP key [count]
// 如果key不存在，返回Nil reply
// 如果只返回1个，类型为Bulk string
// 如果返回多个，类型为MultiBulk string
func SPopCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 2 {
		return protocol.NewArgNumErrReply("SPOP")
	}

	key := string(args[0])
	count := 1
	if len(args) == 2 {
		var err error
		count, err = parseInt(args[1])
		if err != nil {
			return protocol.ErrorSyntaxReply
		}
	}

	set, errReply := d.getEntityAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.NullBulkReply
	}
	members := set.RandomDistinctMembers(count)
	memberBytes := make([][]byte, len(members))
	idx := 0
	for _, member := range members {
		set.Remove(member)
		memberBytes[idx] = []byte(member)
		idx++
	}
	if set.Len() == 0 {
		d.Remove(key)
	}

	return protocol.NewMultiBulkReply(memberBytes)
}

func init() {
	registerNormalCommand("sadd", SAddCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("smembers", SMembersCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("scard", SCardCommand, readFirstKey, 2, tagRead)
	registerNormalCommand("srem", SRemCommand, writeFirstKey, -2, tagWrite)
	registerNormalCommand("sdiff", SDiffCommand, readAllKeys, -2, tagRead)
	registerNormalCommand("sdiffstore", SDiffStoreCommand, prepareSetStore, -3, tagWrite)
	registerNormalCommand("sunion", SUnionCommand, readAllKeys, -2, tagRead)
	registerNormalCommand("sunionstore", SUnionStoreCommand, prepareSetStore, -3, tagWrite)
	registerNormalCommand("sinter", SInterCommand, readAllKeys, -2, tagRead)
	registerNormalCommand("sinterstore", SInterStoreCommand, prepareSetStore, -3, tagWrite)
	registerNormalCommand("sintercard", SInterCardCommand, prepareSInterCard, -3, tagRead)
	registerNormalCommand("sismember", SIsMemberCommand, readFirstKey, 3, tagRead)
	registerNormalCommand("smismember", SMIsMemberCommand, readFirstKey, -3, tagRead)
	registerNormalCommand("smove", SMoveCommand, prepareSMove, 4, tagWrite)
	registerNormalCommand("srandmember", SRandMemberCommand, readFirstKey, -2, tagRead)
	registerNormalCommand("spop", SPopCommand, writeFirstKey, -2, tagWrite)
	// sscan 这个命令比较复杂，暂不实现 https://www.lixueduan.com/posts/redis/redis-scan/
}
