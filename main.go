package main

import (
	"git.code.oa.com/trpc-go/trpc-go/log"
	_ "github.com/lovelydayss/goredis/config"
	"github.com/lovelydayss/goredis/server"
)

func main() {

	server, err := server.ConstructServer()
	if err != nil {
		log.Fatal("server construct failed: %s", err.Error())
	}

	if err := server.Run(); err != nil {
		log.Fatal("server run failed: %s", err.Error())
	}

}
