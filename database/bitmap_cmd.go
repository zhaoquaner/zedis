package database

import (
	"github.com/duke-git/lancet/v2/mathutil"
	"math/bits"
	"strings"
	"zedis/datastruct/bitmap"
	"zedis/interface/db"
	"zedis/interface/redis"
	"zedis/redis/protocol"
)

func (d *DB) getEntityAsBitMap(key string) (*bitmap.BitMap, redis.Reply) {
	entity, exists := d.GetEntity(key)
	if !exists {
		return nil, nil
	}
	if entity.Type != db.StringType {
		return nil, protocol.ErrorWrongTypeReply
	}
	return bitmap.NewBitMap(entity.Data.([]byte)), nil
}

func buildBitMapEntity(val []byte) *db.DataEntity {
	return &db.DataEntity{
		Data: val,
		Type: db.StringType,
	}
}

// SetBitCommand 将偏移量位置bit设置为value
// SETBIT key offset value
func SetBitCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := parseInt64(args[1])
	if !checkOffset(offset, err) {
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
	keyExists := bm != nil
	oldValue := false
	if !keyExists {
		bm = bitmap.NewEmptyBitMap()
		oldValue = false
	} else {
		oldValue = bm.GetBit(offset)
	}

	bm.SetBit(offset, value)
	var res int64 = 0
	if oldValue {
		res = 1
	}
	d.PutEntity(key, buildBitMapEntity(*bm))
	return protocol.NewIntReply(res)
}

// GetBitCommand 获取偏移位置的bit值，1返回1,0返回0，如果超出当前索引，则返回0;如果key不存在或为空字符串，则返回0
// GETBIT key offset
func GetBitCommand(d *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := parseInt64(args[1])
	if !checkOffset(offset, err) {
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
// 如果不指定是BYTE还是BIT，则默认为BYTE
// BITCOUNT key [start end [BYTE | BIT]]
func BitCountCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 4 || len(args) == 2 {
		return protocol.NewArgNumErrReply("BITCOUNT")
	}
	var beginArg, endArg int64
	var err error
	if len(args) >= 3 {
		beginArg, err = parseInt64(args[1])
		if err != nil {
			return protocol.NewErrorReply("ERR start is not an integer or out of range")
		}
		endArg, err = parseInt64(args[2])
		if err != nil {
			return protocol.NewErrorReply("ERR start is not an integer or out of range")
		}
	}
	byteMode := true // 以字节为单位 索引
	if len(args) == 4 {
		arg := strings.ToLower(string(args[3]))
		if arg == "byte" {
			byteMode = true
		} else if arg == "bit" {
			byteMode = false
		} else {
			return protocol.ErrorSyntaxReply
		}
	}

	key := string(args[0])
	str, errReply := d.getEntityAsString(key)
	if errReply != nil {
		return errReply
	}
	if str == nil {
		return protocol.ZeroReply
	}
	bm := bitmap.NewBitMap(str)
	var count int64
	var begin, end int64
	if len(args) == 1 {
		begin = 0
		if byteMode {
			end = bm.ByteSize() - 1
		} else {
			end = bm.BitSize() - 1
		}
	} else {
		begin = beginArg
		end = endArg
	}
	if byteMode {
		bm.ForEachByte(begin, end, func(offset int64, val byte) bool {
			count += int64(bits.OnesCount8(val))
			return true
		})
	} else {
		bm.ForEachBit(begin, end, func(offset int64, val bool) bool {
			if val {
				count++
			}
			return true
		})
	}

	return protocol.NewIntReply(count)
}

// BitPosCommand 返回第一个值为bit(只能为0或1)的 bit索引，可以指定start和end，如果不指定，默认从0开始遍历
// 如果指定了BYTE或BIT模式，则指定索引以BYTE或BIT为单位，不指定默认为BYTE
// BITPOS key bit [start [end [BYTE | BIT]]]
func BitPosCommand(d *DB, args [][]byte) redis.Reply {
	if len(args) > 5 {
		return protocol.NewArgNumErrReply("BITPOS")
	}

	key := string(args[0])
	bitValue := false
	if string(args[1]) == "1" {
		bitValue = true
	} else if string(args[1]) == "0" {
		bitValue = false
	} else {
		return protocol.NewErrorReply("ERR The bit argument must be 1 or 0.")
	}

	byteMode := true
	if len(args) == 5 {
		if strings.ToLower(string(args[4])) == "byte" {
			byteMode = true
		} else if strings.ToLower(string(args[4])) == "bit" {
			byteMode = false
		} else {
			return protocol.ErrorSyntaxReply
		}
	}

	bm, errReply := d.getEntityAsBitMap(key)
	if errReply != nil {
		return errReply
	}
	if bm == nil {
		if bitValue {
			return protocol.NewIntReply(-1)
		} else {
			return protocol.ZeroReply
		}
	}

	var start int64 = 0
	var err error
	if len(args) >= 3 {
		start, err = parseInt64(args[2])
		if err != nil {
			return protocol.NewErrorReply("ERR value is not an integer or out of range")
		}
	}
	end := bm.ByteSize() - 1
	if len(args) >= 4 {
		end, err = parseInt64(args[3])
		if err != nil {
			return protocol.NewErrorReply("ERR value is not an integer or out of range")
		}

	}
	if byteMode {
		start *= 8
		end = (end+1)*8 - 1
	}
	var resIndex int64 = -1
	bm.ForEachBit(start, end, func(offset int64, val bool) bool {
		if val == bitValue {
			resIndex = offset
			return false
		}
		return true
	})
	return protocol.NewIntReply(resIndex)
}

// BitOpCommand 执行位运算，并将结果存入destKey中，返回存入destKey字节数组的长度(即最长字节数组的长度)
// BITOP <AND | OR | XOR | NOT> destKey key [key ...]
// 其中如果位运算为NOT 则只能指定一个key，即格式为：BITOP NOT destkey srckey
// 如果指定的多个key长度不一致，则以最长的字节数组为准，其他的字节数组都在后面补0
// 如果指定的key不存在，则视为字节数组每一个byte都是0
func BitOpCommand(d *DB, args [][]byte) redis.Reply {
	op := strings.ToUpper(string(args[0]))
	if op != "AND" && op != "OR" && op != "XOR" && op != "NOT" {
		return protocol.ErrorSyntaxReply
	}
	if op == "NOT" && len(args) > 3 {
		return protocol.NewArgNumErrReply("BITOP")
	}

	destKey := string(args[1])

	key1 := string(args[2])
	byteArray1, errReply := d.getEntityAsString(key1)
	if errReply != nil {
		return errReply
	}
	if op == "NOT" {
		if len(byteArray1) == 0 {
			return protocol.ZeroReply
		}
		res := make([]byte, len(byteArray1))
		for i, b := range byteArray1 {
			res[i] = ^b
		}
		d.PutEntity(destKey, BuildStringEntity(res))
		return protocol.NewIntReply(int64(len(res)))
	}

	res := byteArray1
	for i := 3; i < len(args); i++ {
		key := string(args[3])
		array, errReply := d.getEntityAsString(key)
		if errReply != nil {
			return errReply
		}
		res = opByteArray(res, array, op)
	}
	if len(res) == 0 {
		return protocol.ZeroReply
	}
	d.PutEntity(destKey, buildBitMapEntity(res))
	return protocol.NewIntReply(int64(len(res)))
}

func init() {
	registerNormalCommand("setbit", SetBitCommand, writeFirstKey, 4, tagWrite)
	registerNormalCommand("getbit", GetBitCommand, readFirstKey, 3, tagRead)

	registerNormalCommand("bitcount", BitCountCommand, readFirstKey, -2, tagRead)
	registerNormalCommand("bitpos", BitPosCommand, readFirstKey, -3, tagRead)
	registerNormalCommand("bitop", BitOpCommand, prepareBitOp, -4, tagWrite)
	// BITFIELD、BITFIELD_RO有点复杂，后面再实现
}

func checkOffset(index int64, err error) bool {
	if err != nil {
		return false
	}
	if index < 0 || index >= 2<<32 {
		return false
	}
	return true
}

func opByteArray(a1 []byte, a2 []byte, op string) []byte {
	resLen := mathutil.Max(len(a1), len(a2))
	res := make([]byte, resLen)
	for i := 0; i < resLen; i++ {
		var b1, b2 byte
		if i < len(a1) {
			b1 = a1[i]
		} else {
			b1 = 0x00
		}

		if i < len(a2) {
			b2 = a2[i]
		} else {
			b2 = 0x00
		}

		switch op {
		case "AND":
			res[i] = b1 & b2
		case "OR":
			res[i] = b1 | b2
		case "XOR":
			res[i] = b1 ^ b2
		}

	}
	return res
}
