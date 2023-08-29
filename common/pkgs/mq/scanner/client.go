package scanner

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQClient
}

func NewClient(cfg *stgmq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQClient(cfg.MakeConnectingURL(), stgmq.SCANNER_QUEUE_NAME, "")
	if err != nil {
		return nil, err
	}

	return &Client{
		rabbitCli: rabbitCli,
	}, nil
}

func (c *Client) Close() {
	c.rabbitCli.Close()
}

type PoolClient struct {
	*Client
	owner *Pool
}

func (c *PoolClient) Close() {
	c.owner.Release(c)
}

type Pool struct {
	mqcfg *stgmq.Config
}

func NewPool(mqcfg *stgmq.Config) *Pool {
	return &Pool{
		mqcfg: mqcfg,
	}
}
func (p *Pool) Acquire() (*PoolClient, error) {
	cli, err := NewClient(p.mqcfg)
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
