package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/agent/config"
	agentserver "gitlink.org.cn/cloudream/proto"
	"gitlink.org.cn/cloudream/utils/ipfs"
	"gitlink.org.cn/cloudream/utils/logger"

	"google.golang.org/grpc"

	rasvr "gitlink.org.cn/cloudream/rabbitmq/server"
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

	err = logger.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	ipfs, err := ipfs.NewIPFS(config.Cfg().IPFSPort)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(2)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	cmdSvr, err := rasvr.NewAgentServer(NewCommandService(ipfs), 0)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	go serveCommandServer(cmdSvr, &wg)

	go reportStatus(&wg) //网络延迟感知

	//面向客户端收发数据
	listenAddr := fmt.Sprintf("127.0.0.1:%d", config.Cfg().GRPCPort)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}

	s := grpc.NewServer()
	agentserver.RegisterTranBlockOrReplicaServer(s, NewGPRCService(ipfs))
	s.Serve(lis)

	wg.Wait()
}

func serveCommandServer(server *rasvr.AgentServer, wg *sync.WaitGroup) {
	server.Serve()
	wg.Done()
}
