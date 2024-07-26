package database

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"git.code.oa.com/trpc-go/trpc-go/log"
	def "github.com/lovelydayss/goredis/interface"
)

// Handler 是命令分发的具体实现
type Handler struct {
	sync.Once
	mu     sync.RWMutex
	conns  map[net.Conn]struct{}
	closed atomic.Bool

	db        def.DB
	parser    def.Parser
	persister def.Persister
}

// NewHandler 初始化
func NewHandler(db def.DB, persister def.Persister, parser def.Parser) (def.Handler, error) {
	h := Handler{
		conns:     make(map[net.Conn]struct{}),
		persister: persister,
		db:        db,
		parser:    parser,
	}

	return &h, nil
}

// Start 指令分发层启动
func (h *Handler) Start() error {
	// 加载持久化文件，还原内容
	reloader, err := h.persister.Reloader()
	if err != nil {
		return err
	}
	defer reloader.Close()

	// 读取持久化文件，还原数据库
	h.handle(def.SetLoadingPattern(context.Background()), def.NewFakeReaderWriter(reloader))
	return nil
}

// Close 关闭指令分发层
func (h *Handler) Close() {
	h.Once.Do(func() {
		log.Warnf("[handler]handler closing...")
		h.closed.Store(true)
		h.mu.RLock()
		defer h.mu.RUnlock()
		for conn := range h.conns {
			if err := conn.Close(); err != nil {
				log.Errorf("[handler]close conn err, local addr: %s, err: %s", conn.LocalAddr().String(), err.Error())
			}
		}
		h.conns = nil
		h.db.Close()
		h.persister.Close()
	})
}

// Handle 处理连接
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	h.mu.Lock()
	// 判断 db 是否已经关闭
	if h.closed.Load() {
		h.mu.Unlock()
		return
	}

	// 当前 conn 缓存起来
	h.conns[conn] = struct{}{}
	h.mu.Unlock()

	// 进一步调用
	h.handle(ctx, conn)
}

// handle 处理请求
func (h *Handler) handle(ctx context.Context, conn io.ReadWriter) {

	// 逐个处理 conn 中请求指令-协程并发
	stream := h.parser.ParseStream(conn)

	for {
		select {
		case <-ctx.Done():
			log.Warnf("[handler]handle ctx err: %s", ctx.Err().Error())
			return

		// chan 解耦，有指令到达对指令处理
		case droplet := <-stream:
			if err := h.handleDroplet(ctx, conn, droplet); err != nil {
				log.Errorf("[handler]conn terminated, err: %s", droplet.Err.Error())
				return
			}
		}
	}
}

// handleDroplet 处理每一笔指令
func (h *Handler) handleDroplet(ctx context.Context, conn io.ReadWriter, droplet *def.Droplet) error {
	if droplet.Terminated() {
		return droplet.Err
	}

	if droplet.Err != nil {
		_, _ = conn.Write(droplet.Reply.ToBytes())
		log.Errorf("[handler]conn request, err: %s", droplet.Err.Error())
		return nil
	}

	if droplet.Reply == nil {
		log.Errorf("[handler]conn empty request")
		return nil
	}

	// 请求参数必须为 multiBulkReply 类型
	multiReply, ok := droplet.Reply.(def.MultiReply)
	if !ok {
		log.Errorf("[handler]conn invalid request: %s", droplet.Reply.ToBytes())
		return nil
	}

	// 调用数据库层进行处理
	if reply := h.db.Do(ctx, multiReply.Args()); reply != nil {

		// 有返回结果，写回 conn
		_, _ = conn.Write(reply.ToBytes())
		return nil
	}

	// 无返回结果，返回未知错误
	_, _ = conn.Write(def.UnknownErrReplyBytes)
	return nil
}
