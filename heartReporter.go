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

	"utils"
	//"google.golang.org/grpc"
	//"github.com/go-ping/ping"
)

func heartReport(wg *sync.WaitGroup) {
	rabbit1 := rabbitmq.NewRabbitMQSimple("coorQueue")

	for {
		//挨个ping其他agent(AgentIpList)，记录延迟到AgentDelay
		ips := utils.GetAgentIps()
		agentDelay := make([]int, len(ips))
		waitG := sync.WaitGroup{}
		waitG.Add(len(ips))
		for i := 0; i < len(ips); i++ {
			go func(i int, wg *sync.WaitGroup) {
				connStatus := utils.GetConnStatus(ips[i])
				fmt.Println(connStatus)
				if connStatus.IsReachable {
					agentDelay[i] = int(connStatus.Delay.Milliseconds()) + 1
				} else {
					agentDelay[i] = -1
				}

				print(agentDelay[i])
				//wg.Wait()
				wg.Done()
			}(i, &waitG)
		}
		waitG.Wait()
		fmt.Println(agentDelay)
		//TODO: 查看本地IPFS daemon是否正常，记录到ipfsStatus
		ipfsStatus := true
		//TODO：访问自身资源目录（配置文件中获取路径），记录是否正常，记录到localDirStatus
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
