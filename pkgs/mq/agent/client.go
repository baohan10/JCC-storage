package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQClient
	id        int64
}

func NewClient(id int64, cfg *mymq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQClient(cfg.MakeConnectingURL(), mymq.MakeAgentQueueName(id), "")
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
