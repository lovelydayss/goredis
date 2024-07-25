package def

import (
	"context"
	"net"
)

// Handler 指令分发层结构体定义
type Handler interface {

	// 启动循环
	Start() error

	// 关闭循环
	Close()

	// 处理请求
	Handle(ctx context.Context, conn net.Conn)
}
