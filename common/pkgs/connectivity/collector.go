package connectivity

import (
	"math/rand"
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type Connectivity struct {
	ToNodeID cdssdk.NodeID
	Delay    *time.Duration
	TestTime time.Time
}

type Collector struct {
	cfg            *Config
	onCollected    func(collector *Collector)
	collectNow     chan any
	close          chan any
	connectivities map[cdssdk.NodeID]Connectivity
	lock           *sync.RWMutex
}

func NewCollector(cfg *Config, onCollected func(collector *Collector)) Collector {
	rpt := Collector{
		cfg:            cfg,
		collectNow:     make(chan any),
		close:          make(chan any),
		connectivities: make(map[cdssdk.NodeID]Connectivity),
		lock:           &sync.RWMutex{},
		onCollected:    onCollected,
	}
	go rpt.serve()
	return rpt
}

func (r *Collector) Get(nodeID cdssdk.NodeID) *Connectivity {
	r.lock.RLock()
	defer r.lock.RUnlock()

	con, ok := r.connectivities[nodeID]
	if ok {
		return &con
	}

	return nil
}
func (r *Collector) GetAll() map[cdssdk.NodeID]Connectivity {
	r.lock.RLock()
	defer r.lock.RUnlock()

	ret := make(map[cdssdk.NodeID]Connectivity)
	for k, v := range r.connectivities {
		ret[k] = v
	}

	return ret
}

// 启动一次收集
func (r *Collector) CollecNow() {
	select {
	case r.collectNow <- nil:
	default:
	}
}

// 就地进行收集，会阻塞当前线程
func (r *Collector) CollectInPlace() {
	r.testing()
}

func (r *Collector) Close() {
	select {
	case r.close <- nil:
	default:
	}
}

func (r *Collector) serve() {
	log := logger.WithType[Collector]("")
	log.Info("start connectivity reporter")

	// 为了防止同时启动的节点会集中进行Ping，所以第一次上报间隔为0-TestInterval秒之间随机
	startup := true
	firstReportDelay := time.Duration(float64(r.cfg.TestInterval) * float64(time.Second) * rand.Float64())
	ticker := time.NewTicker(firstReportDelay)

loop:
	for {
		select {
		case <-ticker.C:
			r.testing()
			if startup {
				startup = false
				ticker.Reset(time.Duration(r.cfg.TestInterval) * time.Second)
			}

		case <-r.collectNow:
			r.testing()

		case <-r.close:
			ticker.Stop()
			break loop
		}
	}

	log.Info("stop connectivity reporter")
}

func (r *Collector) testing() {
	log := logger.WithType[Collector]("")
	log.Debug("do testing")

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getNodeResp, err := coorCli.GetNodes(coormq.NewGetNodes(nil))
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}
	cons := make([]Connectivity, len(getNodeResp.Nodes))
	for i, node := range getNodeResp.Nodes {
		tmpIdx := i
		tmpNode := node

		wg.Add(1)
		go func() {
			defer wg.Done()
			cons[tmpIdx] = r.ping(tmpNode)
		}()
	}

	wg.Wait()

	r.lock.Lock()
	// 删除所有node的记录，然后重建，避免node数量变化时导致残余数据
	r.connectivities = make(map[cdssdk.NodeID]Connectivity)
	for _, con := range cons {
		r.connectivities[con.ToNodeID] = con
	}
	r.lock.Unlock()

	if r.onCollected != nil {
		r.onCollected(r)
	}
}

func (r *Collector) ping(node cdssdk.Node) Connectivity {
	log := logger.WithType[Collector]("").WithField("NodeID", node.NodeID)

	ip := node.ExternalIP
	port := node.ExternalGRPCPort
	if node.LocationID == stgglb.Local.LocationID {
		ip = node.LocalIP
		port = node.LocalGRPCPort
	}

	agtCli, err := stgglb.AgentRPCPool.Acquire(ip, port)
	if err != nil {
		log.Warnf("new agent %v:%v rpc client: %w", ip, port, err)
		return Connectivity{
			ToNodeID: node.NodeID,
			Delay:    nil,
			TestTime: time.Now(),
		}
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	start := time.Now()
	err = agtCli.Ping(*stgglb.Local.NodeID)
	if err != nil {
		log.Warnf("ping: %v", err)
		return Connectivity{
			ToNodeID: node.NodeID,
			Delay:    nil,
			TestTime: time.Now(),
		}
	}

	// 此时间差为一个来回的时间，因此单程延迟需要除以2
	delay := time.Since(start) / 2
	return Connectivity{
		ToNodeID: node.NodeID,
		Delay:    &delay,
		TestTime: time.Now(),
	}
}
