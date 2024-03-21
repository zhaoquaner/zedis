package list

import (
	"strconv"
)

/*
参考：https://juejin.cn/post/6914456200650162189, https://pdai.tech/md/db/nosql-redis/db-redis-x-redis-ds.html#%E5%8E%8B%E7%BC%A9%E5%88%97%E8%A1%A8-ziplist
压缩列表格式为：

 zlBytes | zlTail | zlLen | entry1 | entry2 | ... | entryn | zlEnd

每个entry的格式为:
如果是String类型: previous_entry_length | encoding | content
如果是Int类型: previous_entry_length | encoding
*/

/* ziplist各部分占用字节长度 */
const (
	byteNumOfZlBytes = 4 // 压缩列表占用内存字节数，用4个字节表示
	byteNumOfZlTail  = 4 // 记录了压缩列表尾结点距离压缩列表起始地址有多少字节，用4字节表示
	byteNumOfZlLen   = 2 // 记录了压缩列表entry节点数量，数量小于uint16_max(65535)时，属性值即为压缩列表节点数量，等于该值时，需遍历列表才能得到真实数量
	byteBumOfZlEnd   = 1 // 标记压缩列表末尾，用一个字节表示，固定值0xFF(十进制255)
)

// 压缩列表结束符
const (
	zlEndFlag byte = 0xFF
)

// 表示字符串编码掩码，其中6,14,32表示字符串对应字节数组长度最大为2^6 - 1, 2^14 - 1, 2^32 - 1
const (
	string6EncodingMask  byte = 0b00000000
	string14EncodingMask byte = 0b01000000
	string32EncodingMask byte = 0b10000000
)

const (
	ZipListStrMask byte = 0b11000000
	ZipListIntMask byte = 0b00110000

	ZipListIntImmediateMask byte = 0x0f // 1111xxxx掩码，来提取低4位的bit值
	ZipListIntImmediateMin  byte = 0xf1 // 11110001 1111xxxx编码表达的最小值，即最小值为1
	ZipListIntImmediateMax  byte = 0xfd // 11111101 1111xxxx编码表达的最大值，即最大值为12
)

const (
	int16Encoding byte = 0b11000000 // encoding共3个字节，后2个字节表示一个int16
	int32Encoding byte = 0b11010000 // encoding共5个字节，后4个字节表示一个int32
	int64Encoding byte = 0b11100000 // encoding共9个字节，后8个字节表示一个int64
	int24Encoding byte = 0b11110000 // encoding共4个字节，后3个字节表示一个int24
	int8Encoding  byte = 0b11111110 // encoding共2个字节，后1个字节表示一个int8
	// 如果encoding前两个bit是11，但是不是上述编码时，表示encoding只有1位，格式为：1111xxxx，其中xxxx表示0-12的整数值，即11110001 ~ 11111101，然后减1
	// 因为11111111 、 11110000 、 11111110都被占用了，所以范围只能是11110001 ~ 11111101，同时为了从0开始表示，因此需要减1
)

const (
	ZipListEncodingSizeInvalid byte = 0xff
)

const (
	// 表示previous_entry_length这个字段用5个字节表示，第1个字节固定为0XFE，后4个字节实际表示前一个元素长度
	// 如果第一个字节不是0XFE，则previous_entry_length只用1个字节表示，该字段就表示前一个元素长度
	fiveByteOfPreviousEntryLengthFlag  byte = 0xFE
	byteNumOfPreviousEntryLengthBorder      = 254
)

type ZipList struct {
	data []byte
}

type entry struct {
	preLen   int64  // 前一个entry长度
	encoding []byte // 如果存储的是int，则encoding同时保存类型和值；如果是str，则只保存类型和字符串字节数组长度
	content  []byte // 如果存储的是int，则content为空；如果是str，则保存字符串值
}

func NewZipList(values [][]byte) *ZipList {
	return nil
}

func (z *ZipList) updateZlBytes() {
	byteLen := len(z.data)
	byteArray := toByteArray(int64(byteLen), byteNumOfZlBytes)
	for i := 0; i < byteNumOfZlBytes; i++ {
		z.data[i] = byteArray[i]
	}
}

// entry个数加1
func (z *ZipList) addLength() {
	oldLenBytes := z.getZlLen()
	oldLen := toInt(oldLenBytes)
	newLenBytes := toByteArray(int64(oldLen+1), byteNumOfZlLen)
	for i := 0; i < byteNumOfZlLen; i++ {
		z.data[i+byteNumOfZlBytes+byteNumOfZlTail] = newLenBytes[i]
	}
}

// 更新tail值，参数为新的tail值
func (z *ZipList) updateZlTail(newTail int) {
	newTailBytes := toByteArray(int64(newTail), byteNumOfZlTail)
	for i := 0; i < byteNumOfZlTail; i++ {
		z.data[i+byteNumOfZlBytes+byteNumOfZlLen] = newTailBytes[i]
	}
}

func (z *ZipList) AddFirst(val []byte) int {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) AddLast(val []byte) int {
	return 1
}

func (z *ZipList) Get(index int) (val []byte) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) Set(index int, val []byte) int {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) Insert(index int, val []byte) int {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) Remove(index int) (val []byte) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) RemoveFirst() (val []byte) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) RemoveLast() (val []byte) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) First() (val []byte) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) Last() (val []byte) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) Length() int {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) ForEach(consumer Consumer) {
	//TODO implement me
	panic("implement me")
}

func (z *ZipList) Contains(expected Expected) bool {
	//TODO implement me
	panic("implement me")
}

// buildEntry 给定上一个entry长度，和本次entry要存储的字节数组值，返回一个entry
func buildEntry(preLen int64, value []byte) *entry {
	// 长度大于等于64的字符串，应该转换成linked_list
	if len(value) >= 64 {
		return nil
	}
	e := &entry{
		preLen:   preLen,
		encoding: nil,
		content:  nil,
	}

	e.encoding = getEncoding(value)
	if entryIsStr(e.encoding[0]) {
		e.content = value
	}
	return e
}

func (z *ZipList) parseEntry(offset int) *entry {
	e := &entry{
		preLen:   0,
		encoding: nil,
		content:  nil,
	}

	preLenByteNum := 1
	if z.data[0] == fiveByteOfPreviousEntryLengthFlag {
		preLenByteNum = 5
	}

	newOffset := preLenByteNum + offset
	e.preLen = int64(toInt(z.data[offset:newOffset]))
	//e.encoding, e.content = z.parseEncodingAndValue(newOffset)
	return e
}

//func (z *ZipList) parseEncodingAndValue(offset int) (encoding []byte, value []byte) {
//	if entryIsStr(z.data[offset]) {
//		strByteLen := 0
//		newOffset := 0
//		switch z.data[offset] & ZipListStrMask {
//		case string6EncodingMask:
//			newOffset = offset + 1
//			encoding = append(encoding, z.data[offset:newOffset]...)
//			strByteLen = int(z.data[offset]&(^ZipListStrMask))
//		case string14EncodingMask:
//			newOffset = offset + 2
//			encoding = append(encoding, z.data[offset:newOffset]...)
//			strByteLen = toInt(encoding) & int(^ZipListStrMask)
//		case string32EncodingMask
//		}
//
//
//
//	}
//}

func getEncoding(val []byte) (encoding []byte) {
	number, err := strconv.ParseInt(string(val), 10, 64)
	if err == nil && number >= 0 {
		if number <= 12 {
			encoding = []byte{byte(0b11110000 | number + 1)}
		} else {
			numByteArray := convertToByteArray(uint64(number))
			switch len(numByteArray) {
			case 1:
				encoding = append(encoding, int8Encoding)
			case 2:
				encoding = append(encoding, int8Encoding)
			case 3:
				encoding = append(encoding, int8Encoding)
			case 4:
				encoding = append(encoding, int8Encoding)
			case 8:
				encoding = append(encoding, int8Encoding)
			}
			encoding = append(encoding, numByteArray...)
		}
	} else {
		stringLength := 0
		valLen := len(val)
		if valLen <= 63 {
			stringLength = int(string6EncodingMask) | valLen
		} else if valLen <= 1<<14-1 {
			stringLength = int(string14EncodingMask) | valLen
		} else {
			stringLength = int(string32EncodingMask) | valLen
		}

		encoding = append(encoding, convertToByteArray(uint64(int64(stringLength)))...)
	}
	return
}

func buildPreLenBytes(previousEntryLength int) []byte {
	res := make([]byte, 0)
	previousEntryBytes := convertToByteArray(uint64(int64(previousEntryLength)))
	if previousEntryLength >= byteNumOfPreviousEntryLengthBorder {
		res = append(res, fiveByteOfPreviousEntryLengthFlag)
		for i := 0; i < 4-len(previousEntryBytes); i++ {
			res = append(res, byte(0))
		}
	}
	res = append(res, previousEntryBytes...)
	return res
}

func getEncodingByteLen(encoding byte) int {
	if encoding == int16Encoding || encoding == int24Encoding || encoding == int8Encoding || encoding == int32Encoding || encoding == int64Encoding {
		return 1
	}
	if encoding >= ZipListIntImmediateMin && encoding <= ZipListIntImmediateMax {
		return 1
	}
	if encoding == string6EncodingMask {
		return 1
	}
	if encoding == string14EncodingMask {
		return 2
	}
	if encoding == string32EncodingMask {
		return 5
	}
	return int(ZipListEncodingSizeInvalid)
}

//func getValueFromEntry(e entry) []byte {
//	var encodingAndContent []byte
//	if e[0] == fiveByteOfPreviousEntryLengthFlag {
//		encodingAndContent = e[5:]
//	} else {
//		encodingAndContent = e[1:]
//	}
//
//	v := ""
//	if encodingAndContent[0]>>6 == 0b00000011 {
//		// value为数字
//		switch encodingAndContent[0] {
//		case int8Encoding:
//
//		case int16Encoding:
//		case int24Encoding:
//		case int32Encoding:
//		case int64Encoding:
//
//		case :
//
//		}
//	} else {
//
//	}
//}

// 判断encoding表示的是否为字符串
func entryIsStr(enc byte) bool {
	return (enc & ZipListStrMask) < ZipListStrMask
}

func entryIsInt(enc byte) bool {
	return enc>>6 == 0b11000000
}

func (z *ZipList) getZlBytes() []byte {
	return z.data[:byteNumOfZlBytes]
}

func (z *ZipList) getZlTail() int {
	return toInt(z.data[byteNumOfZlBytes : byteNumOfZlTail+byteNumOfZlBytes])
}

func (z *ZipList) getZlLen() []byte {
	return z.data[byteNumOfZlBytes+byteNumOfZlTail : byteNumOfZlLen+byteNumOfZlTail+byteNumOfZlBytes]
}

func getZlTailOffset() int {
	return byteNumOfZlBytes
}

func getZlLenOffset() int {
	return byteNumOfZlBytes + byteNumOfZlTail
}

func convertToByteArray(num uint64) []byte {
	byteNum := 1
	if num <= 1<<8-1 {
		byteNum = 1
	} else if num <= 1<<16-1 {
		byteNum = 2
	} else if num <= 1<<24-1 {
		byteNum = 3
	} else if num <= 1<<32-1 {
		byteNum = 4
	} else if num <= 1<<64-1 {
		byteNum = 8
	}

	return toByteArray(int64(num), byteNum)
}

func toByteArray(v int64, byteNum int) []byte {
	res := make([]byte, byteNum)
	idx := 0
	for i := byteNum - 1; i >= 0; i-- {
		res[idx] = byte(v >> (8 * i))
		idx++
	}
	return res
}

func toInt(byteArray []byte) int {
	if len(byteArray) == 0 {
		return 0
	}

	res := 0
	l := len(byteArray)
	for i := 0; i < l; i++ {
		res += int(byteArray[i]) << (8 * (l - 1 - i))
	}
	return res
}
