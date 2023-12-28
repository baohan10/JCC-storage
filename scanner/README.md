# Scanner服务

## 目录结构
- `internal`：服务源码。
  - `config`：服务使用的配置文件结构定义。
  - `event`：被投递到队列顺序执行的事件。
  - `mq`：通过rabbitmq对外提供的接口。实现了`common\pkgs\mq\scanner`目录里文件定义的接口。
  - `tickevent`：定时执行的事件。