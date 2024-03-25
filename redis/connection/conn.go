package connection

import (
	"net"
	"sync"
	"time"
	"zedis/lib/sync/wait"
	"zedis/logger"
)

// Connection 表示与redis客户端的一个连接
type Connection struct {
	conn net.Conn

	// 等待直到发送完数据，用于客户端的优雅关闭
	sendingData wait.Wait

	// 当服务端发送响应时的锁
	mu sync.Mutex

	// 密码可能在运行时被配置文件修改，所以存储起来
	password string
	// 表示是否超出最大连接数，如果是，则该字段被置为true，无法执行任何命令
	exceedMaxClients bool
}

func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer c.sendingData.Done()

	return c.conn.Write(bytes)
}

func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.password = ""
	connPool.Put(c)
	return nil
}

func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *Connection) SetPassword(s string) {
	c.password = s
}

func (c *Connection) GetPassword() string {
	return c.password
}

func (c *Connection) SetExceedMaxClients(b bool) {
	c.exceedMaxClients = b
}

func (c *Connection) CheckExceedMaxClients() bool {
	return c.exceedMaxClients
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

var connPool = sync.Pool{
	New: func() any {
		return &Connection{exceedMaxClients: false}
	},
}

func NewConnection(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{conn: conn, password: "", exceedMaxClients: false}
	}
	c.conn = conn
	return c
}
