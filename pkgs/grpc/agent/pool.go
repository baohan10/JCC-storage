package agent

import (
	"fmt"
)

type PoolConfig struct {
	Port int `json:"port"`
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
func (p *Pool) Acquire(ip string) (*PoolClient, error) {
	cli, err := NewClient(fmt.Sprintf("%s:%d", ip, p.grpcCfg.Port))
	if err != nil {
		return nil, err
	}

	return &PoolClient{
		Client: cli,
		owner:  p,
	}, nil
}

func (p *Pool) Release(cli *PoolClient) {
	cli.Close()
}
