package protocol

import (
	"bytes"
	"strconv"
	"zedis/interface/redis"
)

var (
	CRLF = "\r\n" // redis序列化协议的行分隔符
)

/*
定义RESP协议支持的五种数据类型，这里的数据类型与Redis的set、hash、list不是一个概念
是指在Redis客户端和服务端之间的数据传输类型
包括：
1. 单行字符串(SingleString)：首字节为 +, 非二进制安全字符串
2. 错误(Error)：与单行字符串几乎相同，只是响应首字节为 -
3. 整型(Int)：响应首字节为 :
4. 多行字符串(Bulk String): 响应首字节为 $，二进制安全字符串，格式为:$[byte length]\r\n[string]\r\n,
   即 $ 后为字符串字节长度，然后以\r\n结束，后面跟实际字符串值，最后为CRLF；所以空值表示为 $-1\r\n
   注意： 这种类型只能表示一行字符串，而不能表示：$[byte length]\r\n[s1]\r\n[s2]\r\n...
5. 数组(Array): 响应首字节为 *，首字节后为数组元素个数的十进制数，然后以CRLF结尾，后面是每个元素值，空数组为 *0\r\n,数组可以包含混合类型

RESP不同部分总是以 CRLF="\r\n" 结束

Redis的客户端和服务端都以该协议解析发送来的数据，例如服务端向客户端响应的数据为: +OK\r\n，则客户端实际会显示为：OK；其他数据传输类型类似

客户端发来的所有命令都以MultiBulkReply类型接受，即字符串数组来接受；例如redis命令: set k1 v1
服务端接收到的原始数据为: *3\r\n$3\r\nset\r\n$2\r\nk1\r\n$2\r\nv1\r\n

其余类型都用于服务端向客户端响应数据
*/

/* ---- 单行字符串(非二进制安全) ---- */

// SingleReply 表示单行字符串
type SingleReply struct {
	text string
}

func (r *SingleReply) ToBytes() []byte {
	return []byte("+" + r.text + CRLF)
}

func NewSingleReply(text string) *SingleReply {
	return &SingleReply{text: text}
}

func IsOKReply(msg redis.Reply) bool {
	return string(msg.ToBytes()) == "+OK"+CRLF
}

/* ---- 错误 ---- */

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type StandardErrorReply struct {
	text string
}

func (r *StandardErrorReply) Error() string {
	return r.text
}

func (r *StandardErrorReply) ToBytes() []byte {
	return []byte("-" + r.text + CRLF)
}

func NewErrorReply(text string) *StandardErrorReply {
	return &StandardErrorReply{text: text}
}

// IsErrorReply 判断消息是否为错误消息
func IsErrorReply(msg redis.Reply) bool {
	return msg.ToBytes()[0] == '-'
}

/* ---- 整型 ---- */

type IntReply struct {
	number int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.number, 10) + CRLF)
}

func NewIntReply(number int64) *IntReply {
	return &IntReply{number: number}
}

/* ---- 多行字符串(二进制安全) ---- */

// BulkReply 表示多行字符串
type BulkReply struct {
	Text []byte
}

func (r *BulkReply) ToBytes() []byte {
	if r.Text == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Text)) + CRLF + string(r.Text) + CRLF)
}

func NewBulkReply(text []byte) *BulkReply {
	return &BulkReply{Text: text}
}

/* ---- 数组，即混合消息数组，表示包含多种消息类型的数组 ---- */

type ArrayReply struct {
	replies []redis.Reply
}

func (r *ArrayReply) ToBytes() []byte {
	argLen := len(r.replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, reply := range r.replies {
		buf.Write(reply.ToBytes())
	}
	return buf.Bytes()
}

func NewArrayReply(replies []redis.Reply) *ArrayReply {
	return &ArrayReply{replies: replies}
}

/* ---- 多行字符串数组(不属于RESP协议的五种数据类型，但较为常用) ---- */

// MultiBulkReply 表示多行字符串 数组
type MultiBulkReply struct {
	Texts [][]byte
}

// ClientMessage 所有客户端消息均使用MultiBulkReply接受
type ClientMessage = MultiBulkReply

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Texts)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, text := range r.Texts {
		if text == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(text)) + CRLF + string(text) + CRLF)
		}
	}
	return buf.Bytes()
}

func NewMultiBulkReply(texts [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Texts: texts}
}
