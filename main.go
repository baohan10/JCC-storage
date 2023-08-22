package main

import (
	"fmt"
	"os"

	_ "google.golang.org/grpc/balancer/grpclb"

	distlocksvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-client/internal/cmdline"
	"gitlink.org.cn/cloudream/storage-client/internal/config"
	"gitlink.org.cn/cloudream/storage-client/internal/services"
	"gitlink.org.cn/cloudream/storage-client/internal/task"
	"gitlink.org.cn/cloudream/storage-common/globals"
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

	globals.InitLocal(&config.Cfg().Local)
	globals.InitMQPool(&config.Cfg().RabbitMQ)
	globals.InitAgentRPCPool(&config.Cfg().AgentGRPC)
	if config.Cfg().IPFS != nil {
		logger.Infof("IPFS config is not empty, so create a ipfs client")

		globals.InitIPFSPool(config.Cfg().IPFS)
	}

	distlockSvc, err := distlocksvc.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc)

	taskMgr := task.NewManager(distlockSvc)

	svc, err := services.NewService(distlockSvc, &taskMgr)
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

func serveDistLock(svc *distlocksvc.Service) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")
}
