package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"zedis/interface/tcp"
	"zedis/logger"
)

// Config 存储TCP服务端配置
type Config struct {
	Address    string        `yaml:"Address"`
	MaxConnect uint32        `yaml:"MaxConnect"`
	Timeout    time.Duration `yaml:"Timeout"`
}

// ClientCounter 记录当前连接服务端的客户端数量
var ClientCounter int32

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errChan:
			logger.Info(fmt.Printf("accept error: %s", er.Error()))
		}

		logger.Info("server is shutting down...")
		_ = listener.Close() // listener.Accept() 将立即返回err
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				logger.Infof("accept occurs temporary error:%v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errChan <- err
			break
		}

		logger.Info("accept link")
		ClientCounter++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		}()

	}
	waitDone.Wait()
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	// Notify表示sigChan只接收列出的os信号，其余信号不接收
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}

	logger.Infof("bind: %s, start listening...", cfg.Address)
	ListenAndServe(listener, handler, closeChan)
	return nil

}
