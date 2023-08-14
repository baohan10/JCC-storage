package main

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkg/logger"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	mydb "gitlink.org.cn/cloudream/storage-common/pkgs/db"
	sccli "gitlink.org.cn/cloudream/storage-common/pkgs/mq/client/scanner"
	rasvr "gitlink.org.cn/cloudream/storage-common/pkgs/mq/server/coordinator"
	"gitlink.org.cn/cloudream/storage-coordinator/internal/config"
	"gitlink.org.cn/cloudream/storage-coordinator/internal/services"
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

	coorSvr, err := rasvr.NewServer(services.NewService(db, scanner), &config.Cfg().RabbitMQ)
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
