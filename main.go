package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	distsvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-agent/internal/config"
	"gitlink.org.cn/cloudream/storage-agent/internal/task"
	agentserver "gitlink.org.cn/cloudream/storage-common/pkgs/proto"

	"google.golang.org/grpc"

	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"

	cmdsvc "gitlink.org.cn/cloudream/storage-agent/internal/services/cmd"
	grpcsvc "gitlink.org.cn/cloudream/storage-agent/internal/services/grpc"
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

	coorCli, err := coormq.NewClient(&config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	distlock, err := distsvc.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(5)

	taskMgr := task.NewManager(ipfs, coorCli, distlock)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(ipfs, &taskMgr, coorCli), config.Cfg().ID, &config.Cfg().RabbitMQ)
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
	go serveGRPC(s, lis, &wg)

	go serveDistLock(distlock)

	wg.Wait()
}

func serveAgentServer(server *agtmq.Server, wg *sync.WaitGroup) {
	log.Info("start serving command server")

	err := server.Serve()

	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")

	wg.Done()
}

func serveGRPC(s *grpc.Server, lis net.Listener, wg *sync.WaitGroup) {
	log.Info("start serving grpc")

	err := s.Serve(lis)

	if err != nil {
		log.Errorf("grpc stopped with error: %s", err.Error())
	}

	log.Info("grpc stopped")

	wg.Done()
}

func serveDistLock(svc *distsvc.Service) {
	log.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		log.Errorf("distlock stopped with error: %s", err.Error())
	}

	log.Info("distlock stopped")
}
