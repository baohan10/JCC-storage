module coordinator

go 1.18

require proto v0.0.0

require (
	github.com/streadway/amqp v1.0.0
	rabbitmq v0.0.0
)

replace proto => ../proto

replace rabbitmq => ../rabbitmq
