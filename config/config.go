package config

import (
	c "gitlink.org.cn/cloudream/utils/config"
	log "gitlink.org.cn/cloudream/utils/logger"
)

type Config struct {
	GRPCPort       int        `json:"grpcPort"`
	GRCPPacketSize int        `json:"grpcPacketSize"`
	LocalIP        string     `json:"localIP"`
	Logger         log.Config `json:"logger"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad(&cfg)
}

func Cfg() *Config {
	return &cfg
}
