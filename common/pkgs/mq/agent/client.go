package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQTransport
	id        int64
}

func NewClient(id int64, cfg *stgmq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQTransport(cfg.MakeConnectingURL(), stgmq.MakeAgentQueueName(id), "")
	if err != nil {
		return nil, err
	}

	return &Client{
		rabbitCli: rabbitCli,
		id:        id,
	}, nil
}

func (c *Client) Close() {
	c.rabbitCli.Close()
}

type Pool interface {
	Acquire(id int64) (*Client, error)
	Release(cli *Client)
}

type pool struct {
	mqcfg *stgmq.Config
}

func NewPool(mqcfg *stgmq.Config) Pool {
	return &pool{
		mqcfg: mqcfg,
	}
}
func (p *pool) Acquire(id int64) (*Client, error) {
	return NewClient(id, p.mqcfg)
}

func (p *pool) Release(cli *Client) {
	cli.Close()
}
