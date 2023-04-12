module gitlink.org.cn/cloudream/agent

require (
	ec v0.0.0
	github.com/ipfs/go-ipfs-api v0.6.0
	gitlink.org.cn/cloudream/rabbitmq v0.0.0
	gitlink.org.cn/cloudream/utils v0.0.0
	google.golang.org/grpc v1.54.0
	proto v0.0.0
)

require (
	github.com/baohan10/reedsolomon v0.0.0-20230406042632-43574cac9fa7 // indirect
	github.com/beevik/etree v1.1.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20170627025303-887ab5e44cc3 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/go-ping/ping v1.1.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/ipfs/boxo v0.8.0 // indirect
	github.com/ipfs/go-cid v0.4.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.1.0 // indirect
	github.com/libp2p/go-libp2p v0.27.0 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr v0.9.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multicodec v0.8.1 // indirect
	github.com/multiformats/go-multihash v0.2.1 // indirect
	github.com/multiformats/go-multistream v0.4.1 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	github.com/whyrusleeping/tar-utils v0.0.0-20201201191210-20a61371de5b // indirect
	golang.org/x/crypto v0.8.0 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20230403163135-c38d8f061ccd // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
)

go 1.18

// 运行go mod tidy时需要将下面几行取消注释
//replace ec => ../ec
//replace proto => ../proto
//replace gitlink.org.cn/cloudream/rabbitmq => ../rabbitmq
//replace gitlink.org.cn/cloudream/utils => ../utils
