package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"

	"google.golang.org/grpc"

	"gitlink.org.cn/cloudream/storage/common/consts"
	"gitlink.org.cn/cloudream/storage/common/utils"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"

	grpcsvc "gitlink.org.cn/cloudream/storage/agent/internal/services/grpc"
	cmdsvc "gitlink.org.cn/cloudream/storage/agent/internal/services/mq"
)

// TODO 此数据是否在运行时会发生变化？
var AgentIpList []string

func main() {
	// TODO 放到配置里读取
	AgentIpList = []string{"pcm01", "pcm1", "pcm2"}

	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	err = log.Init(&config.Cfg().Logger)
	if err != nil {
		fmt.Printf("init logger failed, err: %s", err.Error())
		os.Exit(1)
	}

	stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitMQPool(&config.Cfg().RabbitMQ)
	stgglb.InitAgentRPCPool(&agtrpc.PoolConfig{
		Port: config.Cfg().GRPC.Port,
	})
	stgglb.InitIPFSPool(&config.Cfg().IPFS)

	distlock, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Fatalf("new ipfs failed, err: %s", err.Error())
	}

	//处置协调端、客户端命令（可多建几个）
	wg := sync.WaitGroup{}
	wg.Add(5)

	taskMgr := task.NewManager(distlock)

	// 启动命令服务器
	// TODO 需要设计AgentID持久化机制
	agtSvr, err := agtmq.NewServer(cmdsvc.NewService(&taskMgr), config.Cfg().ID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError = func(err error) {
		log.Warnf("agent server err: %s", err.Error())
	}
	go serveAgentServer(agtSvr, &wg)

	go reportStatus(&wg) //网络延迟感知

	//面向客户端收发数据
	listenAddr := config.Cfg().GRPC.MakeListenAddress()
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s failed, err: %s", listenAddr, err.Error())
	}

	s := grpc.NewServer()
	agtrpc.RegisterAgentServer(s, grpcsvc.NewService())
	go serveGRPC(s, lis, &wg)

	go serveDistLock(distlock)

	wg.Wait()
}

func serveAgentServer(server *agtmq.Server, wg *sync.WaitGroup) {
	log.Info("start serving command server")

	err := server.Serve()

	if err != nil {
		log.Errorf("command server stopped with error: %s", err.Error())
	}

	log.Info("command server stopped")

	wg.Done()
}

func serveGRPC(s *grpc.Server, lis net.Listener, wg *sync.WaitGroup) {
	log.Info("start serving grpc")

	err := s.Serve(lis)

	if err != nil {
		log.Errorf("grpc stopped with error: %s", err.Error())
	}

	log.Info("grpc stopped")

	wg.Done()
}

func serveDistLock(svc *distlock.Service) {
	log.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		log.Errorf("distlock stopped with error: %s", err.Error())
	}

	log.Info("distlock stopped")
}

func reportStatus(wg *sync.WaitGroup) {
	coorCli, err := coormq.NewClient(&config.Cfg().RabbitMQ)
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
		ipfsStatus := consts.IPFSStateOK
		//TODO：访问自身资源目录（配置文件中获取路径），记录是否正常，记录到localDirStatus
		localDirStatus := consts.StorageDirectoryStateOK

		//发送心跳
		// TODO 由于数据结构未定，暂时不发送真实数据
		coorCli.AgentStatusReport(coormq.NewAgentStatusReportBody(config.Cfg().ID, []int64{}, []int{}, ipfsStatus, localDirStatus))

		time.Sleep(time.Minute * 5)
	}

	coorCli.Close()

	wg.Done()
}
