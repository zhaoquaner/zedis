package database

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
	"zedis/config"
	"zedis/interface/redis"
	"zedis/redis/protocol"
	"zedis/tcp"
)

var zedisVersion = "1.0.0"

// Ping 命令
func Ping(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.PongReplyConst
	} else if len(args) == 1 {
		return protocol.NewSingleReply(string(args[0]))
	}
	return protocol.NewErrorReply("ERR wrong number of arguments for 'ping' command")
}

// Auth 命令
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.NewArgNumErrReply("auth")
	}
	if config.Config.RequirePass == "" {
		return protocol.NewErrorReply("ERR Client send AUTH, but no password is set")
	}
	passwd := string(args[0])
	c.SetPassword(passwd)
	if config.Config.RequirePass != passwd {
		return protocol.NewErrorReply("ERR invalid password")
	}
	return protocol.OKReplyConst
}

func isAuthenticated(c redis.Connection) bool {
	if config.Config.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Config.RequirePass
}

// Info 命令
func Info(engine *Engine, args [][]byte) redis.Reply {
	infoCommandList := make([]string, 0)
	if len(args) >= 2 {
		return protocol.NewArgNumErrReply("info")
	}
	if len(args) == 0 {
		infoCommandList = []string{"server", "client", "cluster", "keyspace"}
	} else if len(args) == 1 {
		section := strings.ToLower(string(args[0]))
		switch section {
		case "server", "client", "cluster", "keyspace":
			infoCommandList = append(infoCommandList, section)
		case "all", "default":
			infoCommandList = append(infoCommandList, "server", "client", "cluster", "keyspace")
		default:
			return protocol.NewErrorReply("Invalid section for 'info' command")
		}

	}

	var buf bytes.Buffer
	for _, section := range infoCommandList {
		buf.Write(GenZedisInfo(section, engine))
	}
	return protocol.NewBulkReply(buf.Bytes())
}

func GenZedisInfo(section string, engine *Engine) []byte {
	startUpTimeFromNow := getZedisRunningTime()
	var buf bytes.Buffer
	switch section {
	case "server":
		buf.WriteString("# Server\r\n")
		buf.WriteString(fmt.Sprintf("redis_version:%s\r\n", zedisVersion))
		buf.WriteString(fmt.Sprintf("redis_mode:%s\r\n", getZedisRunningMode()))
		buf.WriteString(fmt.Sprintf("os:%s %s\r\n", runtime.GOOS, runtime.GOARCH))
		buf.WriteString(fmt.Sprintf("arch_bits:%d\r\n", 32<<(^uint(0)>>63)))
		buf.WriteString(fmt.Sprintf("go_version:%s\r\n", runtime.Version()))
		buf.WriteString(fmt.Sprintf("process_id:%d\r\n", os.Getpid()))
		buf.WriteString(fmt.Sprintf("run_id:%s\r\n", config.Config.RunId))
		buf.WriteString(fmt.Sprintf("tcp_port:%d\r\n", config.Config.Port))
		buf.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", startUpTimeFromNow))
		buf.WriteString(fmt.Sprintf("uptime_in_days:%d\r\n", startUpTimeFromNow/(time.Hour*24)))
		buf.WriteString(fmt.Sprintf("config_file:%s\r\n", config.Config.ConfigFilePath))
	case "client":
		buf.WriteString("# Client\r\n")
		buf.WriteString(fmt.Sprintf("connected_clients:%d\r\n", tcp.ClientCounter))
		buf.WriteString(fmt.Sprintf("maxclients:%d\r\n", config.Config.MaxClients))
	case "cluster":
		buf.WriteString("# Cluster\r\n")
		buf.WriteString("cluster_enabled:0\n")
	}

	return buf.Bytes()
}

func getZedisRunningMode() string {
	return "standalone"
}

func getZedisRunningTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}

func getDBSize(keys, expireKeys, ttl int64) []byte {
	s := fmt.Sprintf("db: keys=%d, expires=%d,avg_ttl=%d\r\n", keys, expireKeys, ttl)
	return []byte(s)
}
