package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"

	// TODO 注册OpUnion，但在mq包中注册会造成循环依赖，所以只能放到这里
	_ "gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"

	"google.golang.org/grpc"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"

	grpcsvc "gitlink.org.cn/cloudream/storage/agent/internal/grpc"
	cmdsvc "gitlink.org.cn/cloudream/storage/agent/internal/mq"
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

	stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&agtrpc.PoolConfig{})
	stgglb.InitIPFSPool(&config.Cfg().IPFS)

	distlock, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	sw := ioswitch.NewSwitch()

	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(4)

	taskMgr := task.NewManager(distlock, &sw)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(&taskMgr, &sw), config.Cfg().ID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		log.Warnf("agent server err: %s", err.Error())
	})

	go serveAgentServer(agtSvr, &wg)

	// go reportStatus(&wg) //网络延迟感知

	//面向客户端收发数据
	listenAddr := config.Cfg().GRPC.MakeListenAddress()
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}

	s := grpc.NewServer()
	agtrpc.RegisterAgentServer(s, grpcsvc.NewService(&sw))
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

func serveDistLock(svc *distlock.Service) {
	log.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		log.Errorf("distlock stopped with error: %s", err.Error())
	}

	log.Info("distlock stopped")
}
