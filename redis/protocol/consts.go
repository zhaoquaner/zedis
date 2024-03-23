package protocol

var nullBulkBytes = []byte("$-1\r\n")

// nullBulkReply 是一个空字符串
type nullBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *nullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

/* ---- emptyMultiBulkReply ---- */

type emptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (m *emptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

/* ---- OKReply ---- */

var okBytes = []byte("+OK\r\n")

type okReply struct{}

var theOkReply = new(okReply)

func (r *okReply) ToBytes() []byte {
	return okBytes
}

/* ---- PongReply ---- */

type pongReply struct{}

var pongBytes = []byte("+PONG\r\n")

func (r *pongReply) ToBytes() []byte {
	return pongBytes
}

/* ---- zero Reply ---- */

type zeroReply struct{}

var zeroBytes = []byte(":0\r\n")

func (r *zeroReply) ToBytes() []byte {
	return zeroBytes
}

var (
	NullBulkReply       = &nullBulkReply{}
	EmptyBulkReply      = NewBulkReply([]byte(""))
	OKReply             = &okReply{}
	PongReply           = &pongReply{}
	ZeroReply           = &zeroReply{}
	EmptyMultiBulkReply = &emptyMultiBulkReply{}
)
