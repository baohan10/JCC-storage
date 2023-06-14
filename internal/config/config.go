package config

import (
	"gitlink.org.cn/cloudream/common/pkg/distlock"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	db "gitlink.org.cn/cloudream/db/config"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
)

type Config struct {
	Logger   log.Config      `json:"logger"`
	DB       db.Config       `json:"db"`
	RabbitMQ racfg.Config    `json:"rabbitMQ"`
	DistLock distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("coordinator", &cfg)
}

func Cfg() *Config {
	return &cfg
}
