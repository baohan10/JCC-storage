package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQClient
}

func NewClient(cfg *mymq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQClient(cfg.MakeConnectingURL(), mymq.COORDINATOR_QUEUE_NAME, "")
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
