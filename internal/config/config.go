package config

import (
	db "gitlink.org.cn/cloudream/db/config"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
	c "gitlink.org.cn/cloudream/utils/config"
	log "gitlink.org.cn/cloudream/utils/logger"
)

type Config struct {
	Logger   log.Config   `json:"logger"`
	DB       db.Config    `json:"db"`
	RabbitMQ racfg.Config `json:"rabbitMQ"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad(&cfg)
}

func Cfg() *Config {
	return &cfg
}
