package config

import "gitlink.org.cn/cloudream/utils/config"

type Config struct {
	GRPCPort           int   `json:"grpcPort"`
	GRCPPacketSize     int64 `json:"grpcPacketSize"`
	IPFSPort           int   `json:"ipfsPort"`
	MaxReplicateNumber int   `json:"maxReplicateNumber"`
}

var cfg Config

func Init() error {
	return config.DefaultLoad(&cfg)
}

func Cfg() *Config {
	return &cfg
}
