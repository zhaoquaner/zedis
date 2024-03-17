package database

import (
	"strings"
	"zedis/interface/redis"
)

var cmdTable = make(map[string]*command)

const (
	tagWrite = 1 << iota
	tagRead
	tagSpecial
)

// PrepareFunc 执行命令前的操作，返回write keys和read keys
type PrepareFunc func(args [][]byte) ([]string, []string)
type ExecFunc func(db *DB, args [][]byte) redis.Reply

type command struct {
	name     string
	executor ExecFunc
	prepare  PrepareFunc

	// 表示命令参数数量限制，如果arity < 0，则表示参数数量大于等于-arity
	// 例如 get命令 arity为2; mget命令 arity -2
	arity int
	tags  int
}

// registerNormalCommand 注册一个普通Command
func registerNormalCommand(name string, executor ExecFunc, prepare PrepareFunc, arity, tags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		arity:    arity,
		tags:     tags,
	}
	cmdTable[name] = cmd
	return cmd
}

// registerSpecialCommand 注册一个特殊命令，例如keys..
func registerSpecialCommand(name string, arity, tags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:  name,
		arity: arity,
		tags:  tags | tagSpecial,
	}
	cmdTable[name] = cmd
	return cmd
}
