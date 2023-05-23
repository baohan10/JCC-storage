package config

import (
	c "gitlink.org.cn/cloudream/common/utils/config"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	log "gitlink.org.cn/cloudream/common/utils/logger"
	dbcfg "gitlink.org.cn/cloudream/db/config"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
)

type Config struct {
	ID                int          `json:"id"`
	GRPCListenAddress string       `json:"grpcListenAddress"`
	LocalIP           string       `json:"localIP"`
	StorageBaseDir    string       `json:"storageBaseDir"`
	Logger            log.Config   `json:"logger"`
	RabbitMQ          racfg.Config `json:"rabbitMQ"`
	IPFS              ipfs.Config  `json:"ipfs"`
	DB                dbcfg.Config `json:"db"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("agent", &cfg)
}

func Cfg() *Config {
	return &cfg
}
