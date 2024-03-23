package protocol

// ArgNumErrReply represents wrong number of arguments for command
type ArgNumErrReply struct {
	Cmd string
}

// ToBytes marshals redis.Reply
func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func (r *ArgNumErrReply) Error() string {
	return "ERR wrong number of arguments for '" + r.Cmd + "' command"
}

// NewArgNumErrReply represents wrong number of arguments for command
func NewArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

// Unknown command error

type UnknownCommandErrReply struct {
	cmdName string
}

func (u *UnknownCommandErrReply) Error() string {
	return "ERR unknown command " + "'" + u.cmdName + "'"
}

func (u *UnknownCommandErrReply) ToBytes() []byte {
	return []byte("-ERR unknown command " + "'" + u.cmdName + "'\r\n")
}

func NewUnknownCommandErrReply(cmdName string) *UnknownCommandErrReply {
	return &UnknownCommandErrReply{cmdName: cmdName}
}

var (
	ErrorUnknownReply         = NewErrorReply("Err unknown")
	ErrorWrongTypeReply       = NewErrorReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrorSyntaxReply          = NewErrorReply("Err syntax error")
	ErrorNoSuchKeyReply       = NewErrorReply("ERR no such key")
	ErrorIndexOutOfRangeReply = NewErrorReply("ERR index out of range")
)
