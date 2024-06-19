package downloader

type Config struct {
	// EC模式的Object的条带缓存数量
	MaxStripCacheCount int `json:"maxStripCacheCount"`
	// 当到下载节点的延迟高于这个值时，该节点在评估时会有更高的分数惩罚，单位：ms
	HighLatencyNodeMs float64 `json:"highLatencyNodeMs"`
}
