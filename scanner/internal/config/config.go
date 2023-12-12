package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/storage/common/pkgs/db/config"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	ECFileSizeThreshold    int64 `json:"ecFileSizeThreshold"`
	NodeUnavailableSeconds int   `json:"nodeUnavailableSeconds"` // 如果节点上次上报时间超过这个值，则认为节点已经不可用

	Logger   log.Config      `json:"logger"`
	DB       db.Config       `json:"db"`
	RabbitMQ stgmq.Config    `json:"rabbitMQ"`
	DistLock distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("scanner", &cfg)
}

func Cfg() *Config {
	return &cfg
}
