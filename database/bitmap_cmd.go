package database

import (
	"zedis/datastruct/bitmap"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/redis/protocol"
)

func (d *DB) getEntityAsBitMap(key string) (bitmap.IBitMap, redis.Reply) {
	entity, exists := d.GetEntity(key)
	if !exists {
		return nil, nil
	}
	if entity.Type != db.BitMapType {
		return nil, protocol.ErrorWrongTypeReply
	}
	return entity.Data.(bitmap.IBitMap), nil
}

func buildBitMapEntity(bm bitmap.IBitMap) *db.DataEntity {
	return &db.DataEntity{
		Data: bm,
		Type: db.BitMapType,
	}
}

// SetBitCommand 将偏移量位置bit设置为value
// SETBIT key offset value
func SetBitCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := parseInt64(args[1])
	if err != nil || offset >= 2<<32 || offset < 0 {
		return protocol.NewErrorReply("ERR bit offset is not an integer or out of range")
	}
	value := false
	if string(args[2]) == "1" {
		value = true
	} else if string(args[2]) == "0" {
		value = false
	} else {
		return protocol.NewErrorReply(" ERR bit is not an integer or out of range")
	}

	bm, errReply := d.getEntityAsBitMap(key)
	if errReply != nil {
		return errReply
	}
	if bm == nil {
		bm := bitmap.NewEmptyBitMap()
		d.PutEntity(key, buildBitMapEntity(bm))
		bm.SetBit(offset, value)
		return protocol.NewIntReply(0)
	}
	oldValue := bm.GetBit(offset)
	bm.SetBit(offset, value)
	var res int64 = 0
	if oldValue {
		res = 1
	}
	return protocol.NewIntReply(res)
}

// GetBitCommand 获取偏移位置的bit值，1返回1,0返回0，如果超出当前索引，则返回0;如果key不存在或为空字符串，则返回0
// GETBIT key offset
func GetBitCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := parseInt64(args[1])
	if err != nil || offset >= 2<<32 || offset < 0 {
		return protocol.NewErrorReply("ERR bit offset is not an integer or out of range")
	}
	bm, errReply := d.getEntityAsBitMap(key)
	if errReply != nil {
		return errReply
	}
	if bm == nil {
		return protocol.ZeroReply
	}
	val := bm.GetBit(offset)
	if val {
		return protocol.NewIntReply(1)
	} else {
		return protocol.ZeroReply
	}
}

// BitCountCommand 统计key字符串 bit值为1的数量
// 可以指定范围start和end，同时指定该范围是BYTE字节索引，还是Bit位索引
// 如果不指定是BYTE还是BIT，则默认为BIT
// BITCOUNT key [start end [BYTE | BIT]]
func BitCountCommand(d *DB, args [][]byte) redis.Reply {
	return nil
}

func init() {
	registerNormalCommand("setbit", SetBitCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("getbit", GetBitCommand, readFirstKey, 3, tagRead)
}
