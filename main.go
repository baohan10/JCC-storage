package main

import (
	"net"
	"sync"

	agentserver "gitlink.org.cn/cloudream/proto"

	"google.golang.org/grpc"

	rasvr "gitlink.org.cn/cloudream/rabbitmq/server"
)

//TODO xh: 读取配置文件，初始化变量，获取packetSizeInBytes、grpc port、ipfs port、最大副本数、本机公网Ip等信息，参照src/utils/config.go

const (
	Port              = ":5010"
	packetSizeInBytes = 10
	LocalIp           = "localhost"
)

var AgentIpList []string

func main() {
	AgentIpList = []string{"pcm01", "pcm1", "pcm2"}
	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(2)

	// 启动命令服务器
	cmdSvr, err := rasvr.NewAgentServer(&CommandService{}, LocalIp)
	if err != nil {
		// TODO 错误日志
		return
	}
	go serveCommandServer(cmdSvr, &wg)

	go reportStatus(&wg) //网络延迟感知

	//面向客户端收发数据
	lis, err := net.Listen("tcp", Port)
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
