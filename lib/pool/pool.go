package pool

import (
	"runtime/debug"
	"strings"

	"github.com/lovelydayss/goredis/log"

	"github.com/panjf2000/ants"
)

var pool *ants.Pool

func init() {
	pool_, err := ants.NewPool(5000, ants.WithPanicHandler(
		func(i interface{}) {
			stackInfo := strings.Replace(string(debug.Stack()), "\n", "", -1)
			log.GetDefaultLogger().Errorf("recover info: %v, stack info: %s", i, stackInfo)
		}))
	if err != nil {
		panic(err)
	}

	pool = pool_
}

func Submit(task func()) {
	pool.Submit(task)
}
