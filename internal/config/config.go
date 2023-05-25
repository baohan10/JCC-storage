package config

import (
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
)

type Config struct {
	ID                int          `json:"id"`
	GRPCListenAddress string       `json:"grpcListenAddress"`
	LocalIP           string       `json:"localIP"`
	StorageBaseDir    string       `json:"storageBaseDir"`
	TempFileLifetime  int          `json:"tempFileLifetime"` // temp状态的副本最多能保持多久时间，单位：秒
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
