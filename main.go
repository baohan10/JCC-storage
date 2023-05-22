package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	log "gitlink.org.cn/cloudream/common/utils/logger"
	agentserver "gitlink.org.cn/cloudream/proto"

	"google.golang.org/grpc"

	rasvr "gitlink.org.cn/cloudream/rabbitmq/server/agent"

	cmdsvc "gitlink.org.cn/cloudream/agent/internal/services/cmd"
	grpcsvc "gitlink.org.cn/cloudream/agent/internal/services/grpc"
)

// TODO 此数据是否在运行时会发生变化？
var AgentIpList []string

func main() {
	// TODO 放到配置里读取
	AgentIpList = []string{"pcm01", "pcm1", "pcm2"}

	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	err = log.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	ipfs, err := ipfs.NewIPFS(&config.Cfg().IPFS)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(2)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := rasvr.NewAgentServer(cmdsvc.NewService(ipfs), config.Cfg().ID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError = func(err error) {
		log.Warnf("agent server err: %s", err.Error())
	}
	go serveAgentServer(agtSvr, &wg)

	go reportStatus(&wg) //网络延迟感知

	//面向客户端收发数据
	listenAddr := config.Cfg().GRPCListenAddress
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}

	s := grpc.NewServer()
	agentserver.RegisterFileTransportServer(s, grpcsvc.NewService(ipfs))
	s.Serve(lis)

	wg.Wait()
}

func serveAgentServer(server *rasvr.AgentServer, wg *sync.WaitGroup) {
	log.Info("start serving command server")

	err := server.Serve()

	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")

	wg.Done()
}
