package main

import (
	//"context"
	//"log"
	//"os"
	//"io"
	//"fmt"
	//"path/filepath"
	//agentserver "proto"

	"encoding/json"
	"fmt"
	"rabbitmq"
	"sync"
	"time"
	//"google.golang.org/grpc"
)

func heartReport(wg *sync.WaitGroup) {
	rabbit1 := rabbitmq.NewRabbitMQSimple("coorQueue")

	for {
		//挨个ping其他agent(AgentIpList)，记录延迟到AgentDelay
		agentDelay := []int{10, 100, 200}
		//访问ipfs，记录是否能正常访问，记录到ipfsStatus
		ipfsStatus := true
		//访问自身资源目录（obs,minio等），记录是否正常，记录到localDirStatus
		localDirStatus := true
		//发送心跳
		command := rabbitmq.HeartReport{
			Ip:             "localhost",
			AgentDelay:     agentDelay,
			IpfsStatus:     ipfsStatus,
			LocalDirStatus: localDirStatus,
		}
		c, _ := json.Marshal(command)
		b := append([]byte("07"), c...)
		fmt.Println(string(b))
		rabbit1.PublishSimple(b)

		time.Sleep(time.Minute * 5)
	}
	rabbit1.Destroy()
	wg.Done()
}
