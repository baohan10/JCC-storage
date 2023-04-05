module coordinator

go 1.18

require (
	rabbitmq v0.0.0
	utils v0.0.0-00010101000000-000000000000
)

require (
	github.com/beevik/etree v1.1.0 // indirect
	github.com/go-ping/ping v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.7.0 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	golang.org/x/net v0.0.0-20210316092652-d523dce5a7f4 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20210315160823-c6e025ad8005 // indirect
)

replace proto => ../proto

replace rabbitmq => ../rabbitmq

replace utils => ../utils
