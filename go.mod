module gitlink.org.cn/cloudream/client

go 1.18

require (
	gitlink.org.cn/cloudream/ec v0.0.0
	gitlink.org.cn/cloudream/proto v0.0.0
	gitlink.org.cn/cloudream/rabbitmq v0.0.0
	gitlink.org.cn/cloudream/utils v0.0.0
	google.golang.org/grpc v1.54.0
)

require (
	github.com/baohan10/reedsolomon v0.0.0-20230406042632-43574cac9fa7 // indirect
	github.com/beevik/etree v1.1.0 // indirect
	github.com/go-ping/ping v1.1.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20230403163135-c38d8f061ccd // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

// 运行go mod tidy时需要将下面几行取消注释
// replace gitlink.org.cn/cloudream/utils => ../utils
//
// replace gitlink.org.cn/cloudream/rabbitmq => ../rabbitmq
//
// replace gitlink.org.cn/cloudream/ec => ../ec
//
// replace gitlink.org.cn/cloudream/proto => ../proto
