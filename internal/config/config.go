package config

import (
	c "gitlink.org.cn/cloudream/common/utils/config"
	log "gitlink.org.cn/cloudream/common/utils/logger"
	db "gitlink.org.cn/cloudream/db/config"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
)

type Config struct {
	MinAvailableRepProportion float32 `json:"minAvailableRepProportion"` // 可用的备份至少要占所有备份的比例，向上去整
	NodeUnavailableSeconds    int     `json:"nodeUnavailableSeconds"`    // 如果节点上次上报时间超过这个值，则认为节点已经不可用

	Logger   log.Config   `json:"logger"`
	DB       db.Config    `json:"db"`
	RabbitMQ racfg.Config `json:"rabbitMQ"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("scanner", &cfg)
}

func Cfg() *Config {
	return &cfg
}
