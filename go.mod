module gitlink.org.cn/cloudream/storage-scanner

go 1.20

require (
	github.com/samber/lo v1.38.1
	github.com/smartystreets/goconvey v1.8.0
	gitlink.org.cn/cloudream/common v0.0.0
	gitlink.org.cn/cloudream/storage-common v0.0.0
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/smartystreets/assertions v1.13.1 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.9 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20230403163135-c38d8f061ccd // indirect
	google.golang.org/grpc v1.54.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

require (
	github.com/antonfisher/nested-logrus-formatter v1.3.1 // indirect
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/imdario/mergo v0.3.15 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/otiai10/copy v1.12.0 // indirect
	github.com/sirupsen/logrus v1.9.2 // indirect
	github.com/streadway/amqp v1.1.0 // indirect
	github.com/zyedidia/generic v1.2.1 // indirect
	golang.org/x/exp v0.0.0-20230519143937-03e91628a987 // indirect
	golang.org/x/sys v0.7.0 // indirect
)

// 运行go mod tidy时需要将下面几行取消注释
replace gitlink.org.cn/cloudream/common => ../../common

replace gitlink.org.cn/cloudream/storage-common => ../storage-common
