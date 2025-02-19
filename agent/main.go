package main

import (
	"fmt"
	"net"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"

	"google.golang.org/grpc"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"

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

	// 启动网络连通性检测，并就地检测一次
	conCol := connectivity.NewCollector(&config.Cfg().Connectivity, func(collector *connectivity.Collector) {
		log := log.WithField("Connectivity", "")

		coorCli, err := stgglb.CoordinatorMQPool.Acquire()
		if err != nil {
			log.Warnf("acquire coordinator mq failed, err: %s", err.Error())
			return
		}
		defer stgglb.CoordinatorMQPool.Release(coorCli)

		cons := collector.GetAll()
		nodeCons := make([]cdssdk.NodeConnectivity, 0, len(cons))
		for _, con := range cons {
			var delay *float32
			if con.Delay != nil {
				v := float32(con.Delay.Microseconds()) / 1000
				delay = &v
			}

			nodeCons = append(nodeCons, cdssdk.NodeConnectivity{
				FromNodeID: *stgglb.Local.NodeID,
				ToNodeID:   con.ToNodeID,
				Delay:      delay,
				TestTime:   con.TestTime,
			})
		}

		_, err = coorCli.UpdateNodeConnectivities(coormq.ReqUpdateNodeConnectivities(nodeCons))
		if err != nil {
			log.Warnf("update node connectivities: %v", err)
		}
	})
	conCol.CollectInPlace()

	distlock, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	sw := exec.NewWorker()

	dlder := downloader.NewDownloader(config.Cfg().Downloader, &conCol)

	taskMgr := task.NewManager(distlock, &conCol, &dlder)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(&taskMgr), config.Cfg().ID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		log.Warnf("agent server err: %s", err.Error())
	})
	go serveAgentServer(agtSvr)

	//面向客户端收发数据
	listenAddr := config.Cfg().GRPC.MakeListenAddress()
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}
	s := grpc.NewServer()
	agtrpc.RegisterAgentServer(s, grpcsvc.NewService(&sw))
	go serveGRPC(s, lis)

	go serveDistLock(distlock)

	foever := make(chan struct{})
	<-foever
}

func serveAgentServer(server *agtmq.Server) {
	log.Info("start serving command server")

	err := server.Serve()

	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveGRPC(s *grpc.Server, lis net.Listener) {
	log.Info("start serving grpc")

	err := s.Serve(lis)

	if err != nil {
		log.Errorf("grpc stopped with error: %s", err.Error())
	}

	log.Info("grpc stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}

func serveDistLock(svc *distlock.Service) {
	log.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		log.Errorf("distlock stopped with error: %s", err.Error())
	}

	log.Info("distlock stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}
