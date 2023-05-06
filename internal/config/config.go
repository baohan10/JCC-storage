package config

import (
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
	c "gitlink.org.cn/cloudream/utils/config"
	"gitlink.org.cn/cloudream/utils/ipfs"
	log "gitlink.org.cn/cloudream/utils/logger"
)

type Config struct {
	ID                int          `json:"id"`
	GRPCListenAddress string       `json:"grpcListenAddress"`
	LocalIP           string       `json:"localIP"`
	StorageBaseDir    string       `json:"storageBaseDir"`
	Logger            log.Config   `json:"logger"`
	RabbitMQ          racfg.Config `json:"rabbitMQ"`
	IPFS              ipfs.Config  `json:"ipfs"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("agent", &cfg)
}

func Cfg() *Config {
	return &cfg
}
