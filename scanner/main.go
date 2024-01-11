package main

import (
	"fmt"
	"os"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/mq"
	"gitlink.org.cn/cloudream/storage/scanner/internal/tickevent"
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

	db, err := db.NewDB(&config.Cfg().DB)
	if err != nil {
		logger.Fatalf("new db failed, err: %s", err.Error())
	}

	stgglb.InitMQPool(&config.Cfg().RabbitMQ)

	wg := sync.WaitGroup{}
	wg.Add(3)

	distlockSvc, err := distlock.NewService(&config.Cfg().DistLock)
	if err != nil {
		logger.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc, &wg)

	eventExecutor := event.NewExecutor(db, distlockSvc)
	go serveEventExecutor(&eventExecutor, &wg)

	agtSvr, err := scmq.NewServer(mq.NewService(&eventExecutor), &config.Cfg().RabbitMQ)
	if err != nil {
		logger.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError(func(err error) {
		logger.Warnf("agent server err: %s", err.Error())
	})

	go serveScannerServer(agtSvr, &wg)

	tickExecutor := tickevent.NewExecutor(tickevent.ExecuteArgs{
		EventExecutor: &eventExecutor,
		DB:            db,
	})
	startTickEvent(&tickExecutor)

	wg.Wait()
}

func serveEventExecutor(executor *event.Executor, wg *sync.WaitGroup) {
	logger.Info("start serving event executor")

	err := executor.Execute()

	if err != nil {
		logger.Errorf("event executor stopped with error: %s", err.Error())
	}

	logger.Info("event executor stopped")

	wg.Done()
}

func serveScannerServer(server *scmq.Server, wg *sync.WaitGroup) {
	logger.Info("start serving scanner server")

	err := server.Serve()

	if err != nil {
		logger.Errorf("scanner server stopped with error: %s", err.Error())
	}

	logger.Info("scanner server stopped")

	wg.Done()
}

func serveDistLock(svc *distlock.Service, wg *sync.WaitGroup) {
	logger.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		logger.Errorf("distlock stopped with error: %s", err.Error())
	}

	logger.Info("distlock stopped")

	wg.Done()
}

func startTickEvent(tickExecutor *tickevent.Executor) {
	// TODO 可以考虑增加配置文件，配置这些任务间隔时间

	interval := 5 * 60 * 1000

	tickExecutor.Start(tickevent.NewBatchAllAgentCheckCache(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckAllPackage(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	// tickExecutor.Start(tickevent.NewBatchCheckAllRepCount(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckAllStorage(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewCheckAgentState(), 5*60*1000, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckPackageRedundancy(), interval, tickevent.StartOption{RandomStartDelayMs: 10 * 60 * 1000})
}
