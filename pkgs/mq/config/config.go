package config

import "fmt"

type Config struct {
	Address  string `json:"address"`
	Account  string `json:"account"`
	Password string `json:"password"`
	VHost    string `json:"vhost"`
}

func (cfg *Config) MakeConnectingURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s%s", cfg.Account, cfg.Password, cfg.Address, cfg.VHost)
}
