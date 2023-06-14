package main

import (
	"fmt"
	"os"

	distlocksvc "gitlink.org.cn/cloudream/common/pkg/distlock/service"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/coordinator/internal/config"
	"gitlink.org.cn/cloudream/coordinator/internal/services"
	mydb "gitlink.org.cn/cloudream/db"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
	rasvr "gitlink.org.cn/cloudream/rabbitmq/server/coordinator"
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

	db, err := mydb.NewDB(&config.Cfg().DB)
	if err != nil {
		log.Fatalf("new db failed, err: %s", err.Error())
	}

	scanner, err := sccli.NewClient(&config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new scanner client failed, err: %s", err.Error())
	}

	distlockSvc, err := distlocksvc.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}

	coorSvr, err := rasvr.NewServer(services.NewService(db, scanner, distlockSvc), &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new coordinator server failed, err: %s", err.Error())
	}

	coorSvr.OnError = func(err error) {
		log.Warnf("coordinator server err: %s", err.Error())
	}

	// 启动服务
	go serveCoorServer(coorSvr)

	forever := make(chan bool)
	<-forever
}

func serveCoorServer(server *rasvr.Server) {
	log.Info("start serving command server")

	err := server.Serve()
	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")
}
