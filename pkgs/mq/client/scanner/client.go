package scanner

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/mq/config"
)

type Client struct {
	rabbitCli *mq.RabbitMQClient
}

func NewClient(cfg *config.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQClient(cfg.MakeConnectingURL(), mymq.SCANNER_QUEUE_NAME, "")
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
