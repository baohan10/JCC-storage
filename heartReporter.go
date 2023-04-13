package main

import (
	"fmt"
	"sync"
	"time"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	"gitlink.org.cn/cloudream/utils"
	"gitlink.org.cn/cloudream/utils/consts"
)

func reportStatus(wg *sync.WaitGroup) {
	coorCli, err := racli.NewCoordinatorClient()
	if err != nil {
		wg.Done()
		// TODO 日志
		return
	}

	// TODO 增加退出死循环的方法
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
		ipfsStatus := consts.IPFS_STATUS_OK
		//TODO：访问自身资源目录（配置文件中获取路径），记录是否正常，记录到localDirStatus
		localDirStatus := consts.LOCAL_DIR_STATUS_OK

		//发送心跳
		coorCli.AgentStatusReport("localhost", agentDelay, ipfsStatus, localDirStatus)

		time.Sleep(time.Minute * 5)
	}

	coorCli.Close()

	wg.Done()
}
