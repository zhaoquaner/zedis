package redis

// Reply 是redis序列化协议的消息
type Reply interface {
	ToBytes() []byte
}
