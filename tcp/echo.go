package tcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"zedis/lib/sync/wait"
	"zedis/logger"
)

// EchoHandler 用于测试
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Bool
}

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

func (e *EchoClient) Close() error {
	e.Waiting.WaitWithTimeout(time.Second * 10)
	_ = e.Conn.Close()
	return nil
}

func (e *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if e.closing.Load() {
		_ = conn.Close()
		return
	}
	client := &EchoClient{
		Conn: conn,
	}
	e.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				e.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		logger.Infof("receive message: %s", msg)
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

func (e *EchoHandler) Close() error {
	logger.Info("handler is shutting down...")
	e.closing.Store(true)
	e.activeConn.Range(func(key, value any) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}
