package grpc

import "fmt"

type Config struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

func (c *Config) MakeListenAddress() string {
	return fmt.Sprintf("%s:%d", c.IP, c.Port)
}
