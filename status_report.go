package main

import (
	"sync"
	"time"

	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	coorcli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils"
	log "gitlink.org.cn/cloudream/utils/logger"
)

func reportStatus(wg *sync.WaitGroup) {
	coorCli, err := coorcli.NewCoordinatorClient(&config.Cfg().RabbitMQ)
	if err != nil {
		wg.Done()
		log.Error("new coordinator client failed, err: %w", err)
		return
	}

	// TODO 增加退出死循环的方法
	for {
		//挨个ping其他agent(AgentIpList)，记录延迟到AgentDelay
		// TODO AgentIP考虑放到配置文件里或者启动时从coor获取
		ips := utils.GetAgentIps()
		agentDelay := make([]int, len(ips))
		waitG := sync.WaitGroup{}
		waitG.Add(len(ips))
		for i := 0; i < len(ips); i++ {
			go func(i int, wg *sync.WaitGroup) {
				connStatus, err := utils.GetConnStatus(ips[i])
				if err != nil {
					wg.Done()
					log.Warnf("ping %s failed, err: %s", ips[i], err.Error())
					return
				}

				log.Debugf("connection status to %s: %+v", ips[i], connStatus)

				if connStatus.IsReachable {
					agentDelay[i] = int(connStatus.Delay.Milliseconds()) + 1
				} else {
					agentDelay[i] = -1
				}

				wg.Done()
			}(i, &waitG)
		}
		waitG.Wait()
		//TODO: 查看本地IPFS daemon是否正常，记录到ipfsStatus
		ipfsStatus := consts.IPFS_STATUS_OK
		//TODO：访问自身资源目录（配置文件中获取路径），记录是否正常，记录到localDirStatus
		localDirStatus := consts.LOCAL_DIR_STATUS_OK

		//发送心跳
		// TODO 由于数据结构未定，暂时不发送真实数据
		coorCli.AgentStatusReport(coormsg.NewAgentStatusReportBody(config.Cfg().ID, []int{}, []int{}, ipfsStatus, localDirStatus))

		time.Sleep(time.Minute * 5)
	}

	coorCli.Close()

	wg.Done()
}
