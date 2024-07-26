package server

import (
	"github.com/lovelydayss/goredis/database"
	"github.com/lovelydayss/goredis/datastore"
	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/parser"
	"github.com/lovelydayss/goredis/persist"

	"go.uber.org/dig"
)

// Container 业务实现方法的容器
var container = dig.New()

func init() {

	/**
	   存储引擎
	**/
	// 数据持久化
	_ = container.Provide(persist.NewPersister)
	// 存储介质
	_ = container.Provide(datastore.NewKVStore)
	// 执行器
	_ = container.Provide(database.NewDBExecutor)
	// 触发器
	_ = container.Provide(database.NewDBTrigger)

	/**
	   逻辑处理层
	**/
	// 协议解析
	_ = container.Provide(parser.NewParser)
	// 指令处理
	_ = container.Provide(database.NewHandler)

	/**
	   服务端
	**/
	_ = container.Provide(NewServer)

}

// ConstructServer 最顶层构造
func ConstructServer() (*Server, error) {

	var h def.Handler
	if err := container.Invoke(func(_h def.Handler) {
		h = _h
	}); err != nil {
		return nil, err
	}

	return NewServer(h), nil
}
