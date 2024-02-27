package agent

import (
	"fmt"
	sync "sync"
)

type PoolConfig struct {
}

type PoolClient struct {
	*Client
	owner *Pool
}

func (c *PoolClient) Close() {
	c.owner.Release(c)
}

type Pool struct {
	grpcCfg *PoolConfig
	shareds map[string]*PoolClient
	lock    sync.Mutex
}

func NewPool(grpcCfg *PoolConfig) *Pool {
	return &Pool{
		grpcCfg: grpcCfg,
		shareds: make(map[string]*PoolClient),
	}
}

// 获取一个GRPC客户端。由于事先不能知道所有agent的GRPC配置信息，所以只能让调用者把建立连接所需的配置都传递进来，
// Pool来决定要不要新建客户端。
func (p *Pool) Acquire(ip string, port int) (*PoolClient, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)

	p.lock.Lock()
	defer p.lock.Unlock()

	cli, ok := p.shareds[addr]
	if !ok {
		c, err := NewClient(addr)
		if err != nil {
			return nil, err
		}
		cli = &PoolClient{
			Client: c,
			owner:  p,
		}
		p.shareds[addr] = cli
	}

	return cli, nil

}

func (p *Pool) Release(cli *PoolClient) {
	// TODO 释放长时间未使用的client
}
