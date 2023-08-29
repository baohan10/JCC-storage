package config

import "fmt"

type Config struct {
	Address      string `json:"address"`
	Account      string `json:"account"`
	Password     string `json:"password"`
	DatabaseName string `json:"databaseName"`
}

func (cfg *Config) MakeSourceString() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=%s",
		cfg.Account,
		cfg.Password,
		cfg.Address,
		cfg.DatabaseName,
		"Asia%2FShanghai",
	)
}
