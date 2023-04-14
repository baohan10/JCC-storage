package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	"gitlink.org.cn/cloudream/agent/config"
	agentserver "gitlink.org.cn/cloudream/proto"
	"gitlink.org.cn/cloudream/utils/logger"

	"google.golang.org/grpc"

	rasvr "gitlink.org.cn/cloudream/rabbitmq/server"
)

// TODO 此数据是否在运行时会发生变化？
var AgentIpList []string

func main() {
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

	AgentIpList = []string{"pcm01", "pcm1", "pcm2"}
	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(2)

	// 启动命令服务器
	cmdSvr, err := rasvr.NewAgentServer(&CommandService{}, config.Cfg().LocalIP)
	if err != nil {
		// TODO 错误日志
		return
	}
	go serveCommandServer(cmdSvr, &wg)

	go reportStatus(&wg) //网络延迟感知

	//面向客户端收发数据
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", config.Cfg().GRPCPort))
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	agentserver.RegisterTranBlockOrReplicaServer(s, &anyOne{})
	s.Serve(lis)
	wg.Wait()
}

func serveCommandServer(server *rasvr.AgentServer, wg *sync.WaitGroup) {
	server.Serve()
	wg.Done()
}
