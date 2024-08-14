package exec

type Worker interface {
	// 获取连接到这个worker的GRPC服务的地址
	GetAddress() string
	// 判断两个worker是否相同
	Equals(worker Worker) bool
}
