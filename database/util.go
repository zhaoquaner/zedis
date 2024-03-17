package database

import (
	"strconv"
	"time"
	"zedis/redis/protocol"
)

// parseTTL 解析key过期时间，duration是时间单位，如果解析失败，返回error reply
func parseTTL(ttlArg []byte, duration time.Duration) (int64, protocol.ErrorReply) {
	ttlNumber, err := strconv.ParseInt(string(ttlArg), 10, 64)
	if err != nil || ttlNumber <= 0 {
		return 0, protocol.NewErrorReply("ERR invalid expire time")
	}
	return duration.Nanoseconds() * ttlNumber, nil
}

// parseInt64 解析字节数组字符串为10进制字符串
func parseInt64(arg []byte) (int64, error) {
	return strconv.ParseInt(string(arg), 10, 64)
}

// parseInt 解析字节数组字符串为10进制字符串
func parseInt(arg []byte) (int, error) {
	return strconv.Atoi(string(arg))
}
