package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racfg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/config"
)

type Config struct {
	ID                int64           `json:"id"`
	GRPCListenAddress string          `json:"grpcListenAddress"`
	GRPCPort          int             `json:"grpcPort"`
	ECPacketSize      int64           `json:"ecPacketSize"`
	LocalIP           string          `json:"localIP"`
	ExternalIP        string          `json:"externalIP"`
	StorageBaseDir    string          `json:"storageBaseDir"`
	TempFileLifetime  int             `json:"tempFileLifetime"` // temp状态的副本最多能保持多久时间，单位：秒
	Logger            log.Config      `json:"logger"`
	RabbitMQ          racfg.Config    `json:"rabbitMQ"`
	IPFS              ipfs.Config     `json:"ipfs"`
	DistLock          distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("agent", &cfg)
}

func Cfg() *Config {
	return &cfg
}
