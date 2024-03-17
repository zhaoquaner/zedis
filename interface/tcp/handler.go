package tcp

import (
	"context"
	"net"
)

type HandlerFunc func(ctx context.Context, conn net.Conn)

// Handler 表示TCP协连接的处理器
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
