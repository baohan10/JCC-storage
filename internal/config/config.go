package config

import (
	c "gitlink.org.cn/cloudream/common/utils/config"
	log "gitlink.org.cn/cloudream/common/utils/logger"
	db "gitlink.org.cn/cloudream/db/config"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
)

type Config struct {
	Logger   log.Config   `json:"logger"`
	DB       db.Config    `json:"db"`
	RabbitMQ racfg.Config `json:"rabbitMQ"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("coordinator", &cfg)
}

func Cfg() *Config {
	return &cfg
}
