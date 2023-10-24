package agent

import (
	"fmt"
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
}

func NewPool(grpcCfg *PoolConfig) *Pool {
	return &Pool{
		grpcCfg: grpcCfg,
	}
}

// 获取一个GRPC客户端。由于事先不能知道所有agent的GRPC配置信息，所以只能让调用者把建立连接所需的配置都传递进来，
// Pool来决定要不要新建客户端。
func (p *Pool) Acquire(ip string, port int) (*PoolClient, error) {
	cli, err := NewClient(fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}

	return &PoolClient{
		Client: cli,
		owner:  p,
	}, nil
}

func (p *Pool) Release(cli *PoolClient) {
	cli.Client.Close()
}
