package redis

// Connection 表示redis客户端的连接
type Connection interface {
	Write([]byte) (int, error)
	Close() error
	RemoteAddr() string
	SetPassword(string)
	GetPassword() string

	SetExceedMaxClients(b bool)
	CheckExceedMaxClients() bool

	Name() string
}
