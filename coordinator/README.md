# Coordinator服务

## 目录结构
- `internal`：服务源码。
  - `config`：服务使用的配置文件结构定义。
  - `mq`：通过rabbitmq对外提供的接口。实现了`common\pkgs\mq\coodinator`目录里文件定义的接口。
