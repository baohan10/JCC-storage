package main

import (
	rasvr "gitlink.org.cn/cloudream/rabbitmq/server"
)

//TODO xh: 读取配置文件，初始化变量

func main() {
	//TODO xh:解析配置文件

	cmdSvr, err := rasvr.NewCoordinatorServer(&CommandService{})
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
