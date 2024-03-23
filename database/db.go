package database

import (
	"time"
	"zedis/datastruct/dict"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/lib/timewheel"
	"zedis/logger"
	"zedis/redis/protocol"
)

// DB 存储数据并执行用户命令
type DB struct {
	// key -> DataEntity
	data *dict.ConcurrentDict
	// key -> expireTime(time.Time)
	ttlMap *dict.ConcurrentDict

	// callbacks
	insertCallback db.KeyEventCallback
	deleteCallback db.KeyEventCallback
}

func makeDB() *DB {
	return &DB{
		data:   dict.NewConcurrentDict(1 << 16),
		ttlMap: dict.NewConcurrentDict(1 << 10),
	}
}

func (d *DB) Exec(c redis.Connection, cmdName string, cmdArgs [][]byte) redis.Reply {
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.NewUnknownCommandErrReply(cmdName)
	}
	if !validateArity(cmd.arity, len(cmdArgs)+1) {
		return protocol.NewArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	executor := cmd.executor
	if prepare != nil {
		writeKeys, readKeys := prepare(cmdArgs)
		d.RWLocks(writeKeys, readKeys)
		defer d.RWUnLocks(writeKeys, readKeys)
	}
	return executor(d, cmdArgs)
}

/* ---- 锁相关方法 ---- */

func (d *DB) RWLocks(writeKeys, readKeys []string) {
	d.data.RWLocks(writeKeys, readKeys)
}

func (d *DB) RWUnLocks(writeKeys, readKeys []string) {
	d.data.RWUnLocks(writeKeys, readKeys)
}

/* ---- 数据访问方法 ---- */

func (d *DB) GetEntity(key string) (*db.DataEntity, bool) {
	raw, ok := d.data.Get(key)
	if !ok {
		return nil, false
	}
	if d.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*db.DataEntity)
	return entity, true
}

// PutEntity 不管key是不是已存在，都用新key-value覆盖或插入
// 插入 返回1
// 覆盖 返回0
func (d *DB) PutEntity(key string, entity *db.DataEntity) int {
	ret := d.data.Put(key, entity)
	if cb := d.insertCallback; ret > 0 && cb != nil {
		cb(0, key, entity)
	}
	return ret
}

// PutEntityIfNotExists 插入key-value键值对，如果key已存在，则不插入
// 插入成功 返回1
// 插入失败，返回0
func (d *DB) PutEntityIfNotExists(key string, entity *db.DataEntity) int {
	ret := d.data.PutIfAbsent(key, entity)
	if cb := d.insertCallback; ret > 0 && cb != nil {
		cb(0, key, entity)
	}
	return ret
}

// PutEntityIfExists 覆盖原有key-value；如果key不存在，则不覆盖和插入
// 覆盖成功 返回1
// 覆盖失败 返回0
func (d *DB) PutEntityIfExists(key string, entity *db.DataEntity) int {
	return d.data.PutIfExists(key, entity)
}

func (d *DB) Exists(key string) bool {
	ok := d.data.Exists(key)
	if ok {
		return !d.IsExpired(key)
	}
	return false
}

func (d *DB) Remove(key string) (*db.DataEntity, int) {
	raw, deleted := d.data.Remove(key)
	var entity *db.DataEntity
	if deleted > 0 {
		// 如果删除成功，则删除到期时间，等同于重置到期时间
		entity = raw.(*db.DataEntity)
		d.Persist(key)
	}
	if cb := d.deleteCallback; entity != nil && cb != nil {
		cb(0, key, entity)
	}

	if entity != nil {
		return entity, deleted
	}
	return nil, deleted
}

func (d *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := d.Remove(key)
		if exists > 0 {
			deleted += exists
		}
	}
	return deleted
}

func (d *DB) ForEach(consumer dict.Consumer) {
	d.data.ForEach(consumer)
}

func (d *DB) Flush() {
	d.data.Clear()
}

// validateArity 验证参数数量
func validateArity(arity, argNum int) bool {
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- TTL 相关方法 ---- */

func genExpireTaskKey(key string) string {
	return "expire:" + key
}

func (d *DB) Expire(key string, delay time.Duration) {
	d.ttlMap.PutWithLock(key, time.Now().Add(delay))
	taskKey := genExpireTaskKey(key)

	timewheel.Delay(delay, taskKey, func() {
		keys := []string{key}
		d.RWLocks(keys, nil)
		defer d.RWUnLocks(keys, nil)

		rawExpireTime, ok := d.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			logger.Infof("the key %s has expired, deleted", key)
			d.Remove(key)
			d.ttlMap.Remove(key)
		}
	})
}

func (d *DB) ExpireByTime(key string, at time.Time) {

	d.ttlMap.PutWithLock(key, at)
	taskKey := genExpireTaskKey(key)
	timewheel.At(at, taskKey, func() {
		keys := []string{key}
		d.RWLocks(keys, nil)
		defer d.RWUnLocks(keys, nil)

		rawExpireTime, ok := d.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			logger.Infof("the key %s has expired, deleted", key)
			d.Remove(key)
		}
	})
}

func (d *DB) Persist(key string) {
	d.ttlMap.Remove(key)
	timewheel.Cancel(genExpireTaskKey(key))
}

// IsExpired check whether a key is expired
func (d *DB) IsExpired(key string) bool {
	rawExpireTime, ok := d.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		d.Remove(key)
	}
	return expired
}
