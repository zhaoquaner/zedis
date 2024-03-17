package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"zedis/database"
	"zedis/logger"
	"zedis/redis/connection"
	"zedis/redis/parser"
	"zedis/redis/protocol"
)

// Handler 实现了tcp.Handle，作为Redis的服务处理器
type Handler struct {
	activeConn sync.Map
	engine     *database.Engine
	closing    atomic.Bool
}

func NewHandler() *Handler {
	return &Handler{
		engine: database.NewEngine(),
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.activeConn.Delete(client)
}

func (h *Handler) Close() error {
	logger.Info("handler is shutting down...")
	h.closing.Store(true)
	h.activeConn.Range(func(key, value any) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	return nil
}

// Handle 接收和执行redis命令
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		_ = conn.Close()
		return
	}

	// 检查是否超出最大客户端数量
	//if tcp.ClientCounter >= int32(config.Config.MaxClients) {
	//	_, _ = conn.Write(protocol.NewErrorReply("ERR exceed the max clients").ToBytes())
	//	_ = conn.Close()
	//	return
	//}

	client := connection.NewConnection(conn)
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Error != nil {
			if payload.Error == io.EOF || errors.Is(payload.Error, io.ErrUnexpectedEOF) || strings.Contains(payload.Error.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}

			errMsg := protocol.NewErrorReply(payload.Error.Error())
			_, err := client.Write(errMsg.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}

		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		strs := make([]string, 0)
		for _, line := range r.Texts {
			strs = append(strs, string(line))
		}
		logger.Infof("cmd: %s", strings.Join(strs, " "))

		reply := h.engine.Exec(client, r.Texts)
		_, _ = client.Write(reply.ToBytes())

	}

}
