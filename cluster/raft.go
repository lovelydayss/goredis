package cluster

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "github.com/lovelydayss/protocol/raft_node/version1"
)

func main() {

	proxy := pb.NewRaftServiceClientProxy()
	req := &pb.EmptyRequest{}
	rsp, err := proxy.Empty(context.Background(), req, client.WithTarget("127.0.0.1:8888"))
	if err != nil {
		panic(err)
	}

	log.Errorf("rsp: %v", rsp)

}
