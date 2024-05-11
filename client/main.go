package main

import (
	"fmt"
	"os"
	"time"

	_ "google.golang.org/grpc/balancer/grpclb"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/client/internal/cmdline"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

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

	stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&config.Cfg().AgentGRPC)
	if config.Cfg().IPFS != nil {
		logger.Infof("IPFS config is not empty, so create a ipfs client")

		stgglb.InitIPFSPool(config.Cfg().IPFS)
	}

	var conCol connectivity.Collector
	if config.Cfg().Local.NodeID != nil {
		//如果client与某个node处于同一台机器，则使用这个node的连通性信息
		coorCli, err := stgglb.CoordinatorMQPool.Acquire()
		if err != nil {
			logger.Warnf("acquire coordinator mq failed, err: %s", err.Error())
			os.Exit(1)
		}
		getCons, err := coorCli.GetNodeConnectivities(coormq.ReqGetNodeConnectivities([]cdssdk.NodeID{*config.Cfg().Local.NodeID}))
		if err != nil {
			logger.Warnf("get node connectivities failed, err: %s", err.Error())
			os.Exit(1)
		}
		consMap := make(map[cdssdk.NodeID]connectivity.Connectivity)
		for _, con := range getCons.Connectivities {
			var delay *time.Duration
			if con.Delay != nil {
				d := time.Duration(*con.Delay * float32(time.Millisecond))
				delay = &d
			}
			consMap[con.FromNodeID] = connectivity.Connectivity{
				ToNodeID: con.ToNodeID,
				Delay:    delay,
			}
		}
		conCol = connectivity.NewCollectorWithInitData(&config.Cfg().Connectivity, nil, consMap)
		logger.Info("use local node connectivities")

	} else {
		// 否则需要就地收集连通性信息
		conCol = connectivity.NewCollector(&config.Cfg().Connectivity, nil)
		conCol.CollectInPlace()
	}

	distlockSvc, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc)

	taskMgr := task.NewManager(distlockSvc, &conCol)

	dlder := downloader.NewDownloader(config.Cfg().Downloader)

	svc, err := services.NewService(distlockSvc, &taskMgr, &dlder)
	if err != nil {
		logger.Warnf("new services failed, err: %s", err.Error())
		os.Exit(1)
	}

	cmds, err := cmdline.NewCommandline(svc)
	if err != nil {
		logger.Warnf("new command line failed, err: %s", err.Error())
		os.Exit(1)
	}

	cmds.DispatchCommand(os.Args[1:])
}

func serveDistLock(svc *distlock.Service) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}
