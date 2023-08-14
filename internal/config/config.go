package config

import (
	"gitlink.org.cn/cloudream/common/pkg/distlock"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/storage-common/pkgs/db/config"
	racfg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/config"
)

type Config struct {
	MinAvailableRepProportion float32 `json:"minAvailableRepProportion"` // 可用的备份至少要占所有备份的比例，向上去整
	NodeUnavailableSeconds    int     `json:"nodeUnavailableSeconds"`    // 如果节点上次上报时间超过这个值，则认为节点已经不可用

	Logger   log.Config      `json:"logger"`
	DB       db.Config       `json:"db"`
	RabbitMQ racfg.Config    `json:"rabbitMQ"`
	DistLock distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("scanner", &cfg)
}

func Cfg() *Config {
	return &cfg
}
