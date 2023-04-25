package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/coordinator/config"
	"gitlink.org.cn/cloudream/coordinator/services"
	mydb "gitlink.org.cn/cloudream/db"
	rasvr "gitlink.org.cn/cloudream/rabbitmq/server/coordinator"
	"gitlink.org.cn/cloudream/utils/logger"
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

	db, err := mydb.NewDB(config.Cfg().DB.MakeSourceString())
	if err != nil {
		log.Fatalf("new db failed, err: %s", err.Error())
	}

	cmdSvr, err := rasvr.NewCoordinatorServer(services.NewService(db))
	if err != nil {
		log.Fatalf("new coordinator server failed, err: %s", err.Error())
	}

	// 启动命令服务器
	go serveCommandServer(cmdSvr)

	forever := make(chan bool)
	<-forever
}

func serveCommandServer(server *rasvr.CoordinatorServer) {
	log.Info("start serving command server")

	err := server.Serve()
	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")
}
