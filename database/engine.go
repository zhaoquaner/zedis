package database

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"zedis/config"
	"zedis/interface/redis"
	"zedis/logger"
	"zedis/redis/protocol"
)

// Engine 是一个redis引擎对象，可以执行所有命令
type Engine struct {
	db *DB
}

func NewEngine() *Engine {
	engine := &Engine{}
	err := os.MkdirAll(config.GetTempDir(), os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	engine.db = makeDB()
	return engine
}

func (e *Engine) Exec(c redis.Connection, cmdLine [][]byte) (res redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("error occurs: %v\n%s", err, string(debug.Stack()))
			res = protocol.ErrorUnknownReply
		}
	}()

	if c.CheckExceedMaxClients() {
		return protocol.NewErrorReply("ERR max number of clients reached")
	}

	cmdName := strings.ToLower(string(cmdLine[0]))
	// 所有命令处理函数，传的都是去掉命令名称的cmdArgs
	cmdArgs := cmdLine[1:]

	if cmdName == "ping" {
		return Ping(c, cmdArgs)
	}
	if cmdName == "auth" {
		return Auth(c, cmdArgs)
	}

	if !isAuthenticated(c) {
		return protocol.NewErrorReply("NOAUTH Authentication required")
	}

	if cmdName == "info" {
		return Info(e, cmdArgs)
	}

	return e.db.Exec(c, cmdName, cmdArgs)

}
