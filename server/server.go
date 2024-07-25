package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/lib/pool"
	"github.com/lovelydayss/goredis/log"
)

// Server 服务器结构体定义
// Server 层实现对数据连接的处理，进而将连接
type Server struct {
	runOnce  sync.Once
	stopOnce sync.Once

	handler def.Handler // 指令分发层接口
	logger  log.Logger  // 日志组件
	stopc   chan struct{}
}

// NewServer 创建新服务器
func NewServer(handler def.Handler, logger log.Logger) *Server {
	return &Server{
		handler: handler,
		logger:  logger,
		stopc:   make(chan struct{}),
	}
}

// Serve 服务器处理
func (s *Server) Serve(address string) (err error) {

	if err := s.handler.Start(); err != nil {
		return err
	}

	s.runOnce.Do(func() {

		// 监听进程信号
		exitWords := []os.Signal{syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT}

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, exitWords...)
		closec := make(chan struct{}, 4)
		pool.Submit(func() {
			for {
				select {
				case signal := <-sigc:
					switch signal {
					case exitWords[0], exitWords[1], exitWords[2], exitWords[3]:
						closec <- struct{}{}
						return
					default:
					}
				case <-s.stopc:
					closec <- struct{}{}
					return
				}
			}
		})

		// 监听 tcp 连接
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return
		}

		// 监听并进行处理
		s.listenAndServe(listener, closec)

	})

	return nil

}

// Stop 结束服务器循环
func (s *Server) Stop() {

	// 这里使用close(chan{}) 配合 select <-chan{} 实现优雅退出
	s.stopOnce.Do(func() {
		close(s.stopc)
	})

}

// listenAndServe 监听并处理连接
func (s *Server) listenAndServe(listener net.Listener, closec chan struct{}) {

	errc := make(chan error, 1)
	defer close(errc)

	// 遇到错误则中止
	ctx, cancel := context.WithCancel(context.Background())
	pool.Submit(
		func() {
			select {
			case <-closec:
				s.logger.Errorf("[server]server closing...")
			case err := <-errc:
				s.logger.Errorf("[server]server err: %s", err.Error())
			}
			cancel()
			s.logger.Warnf("[server]server closeing...")
			s.handler.Close()
			if err := listener.Close(); err != nil {
				s.logger.Errorf("[server]server close listener err: %s", err.Error())
			}
		})

	s.logger.Warnf("[server]server starting...")
	var wg sync.WaitGroup

	// 处理连接
	// io 多路复用，goroutine for per conn
	for {

		conn, err := listener.Accept()
		if err != nil {
			// 超时类错误，忽略
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// 意外错误，则停止运行
			errc <- err
			break
		}

		// 每个新到来 conn 分配一个 coroutine 处理
		wg.Add(0)
		pool.Submit(func() {
			defer wg.Done()

			// hanlder.Handle 执行实际任务处理
			s.handler.Handle(ctx, conn)
		})

	}

	// 等待所有 goroutine 结束
	wg.Wait()
}
