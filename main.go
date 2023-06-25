package main

import (
	"fmt"
	"os"
	"sync"

	distlocksvc "gitlink.org.cn/cloudream/common/pkg/distlock/service"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db"
	scsvr "gitlink.org.cn/cloudream/rabbitmq/server/scanner"
	"gitlink.org.cn/cloudream/scanner/internal/config"
	"gitlink.org.cn/cloudream/scanner/internal/event"
	"gitlink.org.cn/cloudream/scanner/internal/services"
	"gitlink.org.cn/cloudream/scanner/internal/tickevent"
)

func main() {
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

	db, err := db.NewDB(&config.Cfg().DB)
	if err != nil {
		log.Fatalf("new db failed, err: %s", err.Error())
	}

	wg := sync.WaitGroup{}
	wg.Add(3)

	distlockSvc, err := distlocksvc.NewService(&config.Cfg().DistLock)
	if err != nil {
		log.Warnf("new distlock service failed, err: %s", err.Error())
		os.Exit(1)
	}
	go serveDistLock(distlockSvc, &wg)

	eventExecutor := event.NewExecutor(db, distlockSvc)
	go serveEventExecutor(&eventExecutor, &wg)

	agtSvr, err := scsvr.NewServer(services.NewService(&eventExecutor), &config.Cfg().RabbitMQ)
	if err != nil {
		log.Fatalf("new agent server failed, err: %s", err.Error())
	}
	agtSvr.OnError = func(err error) {
		log.Warnf("agent server err: %s", err.Error())
	}
	go serveScannerServer(agtSvr, &wg)

	tickExecutor := tickevent.NewExecutor(tickevent.ExecuteArgs{
		EventExecutor: &eventExecutor,
		DB:            db,
	})
	startTickEvent(&tickExecutor)

	wg.Wait()
}

func serveEventExecutor(executor *event.Executor, wg *sync.WaitGroup) {
	log.Info("start serving event executor")

	err := executor.Execute()

	if err != nil {
		log.Errorf("event executor stopped with error: %s", err.Error())
	}

	log.Info("event executor stopped")

	wg.Done()
}

func serveScannerServer(server *scsvr.Server, wg *sync.WaitGroup) {
	log.Info("start serving scanner server")

	err := server.Serve()

	if err != nil {
		log.Errorf("scanner server stopped with error: %s", err.Error())
	}

	log.Info("scanner server stopped")

	wg.Done()
}

func serveDistLock(svc *distlocksvc.Service, wg *sync.WaitGroup) {
	log.Info("start serving distlock")

	err := svc.Serve()

	if err != nil {
		log.Errorf("distlock stopped with error: %s", err.Error())
	}

	log.Info("distlock stopped")

	wg.Done()
}

func startTickEvent(tickExecutor *tickevent.Executor) {
	// TODO 可以考虑增加配置文件，配置这些任务间隔时间

	interval := 5 * 60 * 1000

	tickExecutor.Start(tickevent.NewBatchAllAgentCheckCache(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckAllObject(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckAllRepCount(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewBatchCheckAllStorage(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewCheckAgentState(), 5*60*1000, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})

	tickExecutor.Start(tickevent.NewCheckCache(), interval, tickevent.StartOption{RandomStartDelayMs: 60 * 1000})
}
