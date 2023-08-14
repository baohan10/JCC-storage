package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/mq/config"
)

type Client struct {
	rabbitCli *mq.RabbitMQClient
	id        int64
}

func NewClient(id int64, cfg *config.Config) (*Client, error) {
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
