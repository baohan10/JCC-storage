package main

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/config"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/mq"
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
		logger.Fatalf("new db failed, err: %s", err.Error())
	}

	coorSvr, err := coormq.NewServer(mq.NewService(db), &config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new coordinator server failed, err: %s", err.Error())
	}

	coorSvr.OnError(func(err error) {
		logger.Warnf("coordinator server err: %s", err.Error())
	})

	// 启动服务
	go serveCoorServer(coorSvr)

	forever := make(chan bool)
	<-forever
}

func serveCoorServer(server *coormq.Server) {
	logger.Info("start serving command server")

	err := server.Serve()
	if err != nil {
		logger.Errorf("command server stopped with error: %s", err.Error())
	}

	logger.Info("command server stopped")

	// TODO 仅简单结束了程序
	os.Exit(1)
}
