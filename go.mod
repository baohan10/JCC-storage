module client

go 1.18

require proto v0.0.0

replace proto => ../proto

replace rabbitmq => ../rabbitmq

replace ec => ../ec

replace utils => ../utils

require (
	utils v0.0.0-00010101000000-000000000000
	ec v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.53.0
	rabbitmq v0.0.0
)

require (
	github.com/beevik/etree v1.1.0 // indirect
	github.com/go-ping/ping v1.1.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.1.1 // indirect
	github.com/klauspost/reedsolomon v1.11.7 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	golang.org/x/net v0.5.0 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/text v0.6.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)
