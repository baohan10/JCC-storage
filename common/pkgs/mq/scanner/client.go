package scanner

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQTransport
}

func NewClient(cfg *stgmq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQTransport(cfg.MakeConnectingURL(), stgmq.SCANNER_QUEUE_NAME, "")
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

type Pool interface {
	Acquire() (*Client, error)
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
func (p *pool) Acquire() (*Client, error) {
	return NewClient(p.mqcfg)
}

func (p *pool) Release(cli *Client) {
	cli.Close()
}
