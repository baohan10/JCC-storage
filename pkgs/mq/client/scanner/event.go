package scanner

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	scmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/scanner"
)

func (cli *Client) PostEvent(event any, isEmergency bool, dontMerge bool, opts ...mq.SendOption) error {
	opt := mq.SendOption{
		Timeout: time.Second * 30,
	}
	if len(opts) > 0 {
		opt = opts[0]
	}

	body, err := scmsg.NewPostEvent(event, isEmergency, dontMerge)
	if err != nil {
		return fmt.Errorf("new post event body failed, err: %w", err)
	}

	return mq.Send(cli.rabbitCli, body, opt)
}
