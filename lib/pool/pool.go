package pool

import (
	"runtime/debug"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"github.com/panjf2000/ants"
)

var pool = &ants.Pool{}
var err error

func init() {
	pool, err = ants.NewPool(5000, ants.WithPanicHandler(
		func(i interface{}) {
			stackInfo := strings.Replace(string(debug.Stack()), "\n", "", -1)
			log.Errorf("recover info: %v, stack info: %s", i, stackInfo)
		}))
	if err != nil {
		log.Fatal(err)
	}
}

// Submit 提交任务
func Submit(task func()) {
	pool.Submit(task)
}
