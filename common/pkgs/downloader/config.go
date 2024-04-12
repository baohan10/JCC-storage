package downloader

type Config struct {
	// EC模式的Object的条带缓存数量
	MaxStripCacheCount int `json:"maxStripCacheCount"`
}
