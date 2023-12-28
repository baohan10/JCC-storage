# Agent服务

## 目录结构
- `internal`：服务源码。
  - `config`：服务使用的配置文件结构定义。
  - `grpc`：通过grpc对外提供的接口。实现了.proto文件里定义的接口，这个文件位于`common\pkgs\grpc\agent\agent.proto`。
  - `mq`：通过rabbitmq对外提供的接口。实现了`common\pkgs\mq\agent`目录里文件定义的接口。
  - `task`：需要在后台异步运行的任务。

 