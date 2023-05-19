module gitlink.org.cn/cloudream/coordinator

go 1.18

require (
	github.com/samber/lo v1.38.1
	gitlink.org.cn/cloudream/common v0.0.0
	gitlink.org.cn/cloudream/db v0.0.0
	gitlink.org.cn/cloudream/rabbitmq v0.0.0
	gitlink.org.cn/cloudream/utils v0.0.0
)

require (
	github.com/antonfisher/nested-logrus-formatter v1.3.1 // indirect
	github.com/beevik/etree v1.1.0 // indirect
	github.com/go-ping/ping v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.7.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/imdario/mergo v0.3.15 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	golang.org/x/exp v0.0.0-20220303212507-bbda1eaf7a17 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
)

// 运行go mod tidy时需要将下面几行取消注释
// replace gitlink.org.cn/cloudream/rabbitmq => ../rabbitmq
// 
// replace gitlink.org.cn/cloudream/utils => ../utils
// 
// replace gitlink.org.cn/cloudream/common => ../common
// 
// replace gitlink.org.cn/cloudream/db => ../db
