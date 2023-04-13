package main

import (
	mydb "gitlink.org.cn/cloudream/db"
	rasvr "gitlink.org.cn/cloudream/rabbitmq/server"
)

const (
	s_DATABASE_SOURCE_NAME = "root:123456@tcp(127.0.0.1:3306)/kx?charset=utf8mb4&parseTime=true"
)

//TODO xh: 读取配置文件，初始化变量

func main() {
	//TODO xh:解析配置文件
	db, err := mydb.NewDB(s_DATABASE_SOURCE_NAME)
	if err != nil {
		// TODO 错误处理
		return
	}

	cmdSvr, err := rasvr.NewCoordinatorServer(NewCommandService(db))
	if err != nil {
		// TODO 错误日志
		return
	}

	// 启动命令服务器
	go serveCommandServer(cmdSvr)

	forever := make(chan bool)
	<-forever
}

func serveCommandServer(server *rasvr.CoordinatorServer) {
	server.Serve()
}
