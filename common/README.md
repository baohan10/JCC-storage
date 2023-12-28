# 公共库
这个目录存放的是在storage仓库的几个程序之间共享的代码和数据结构定义。

## 目录结构
- `assets`：存放程序会读取使用的配置文件等。会在编译时一并复制到输出目录。
  - `confs`：服务的配置文件。
  - `scripts`：脚本文件。
- `consts`：常量定义。
- `globals`：全局变量定义，主要是各种客户端的Pool。
- `magefiles`：mage工具的脚本。
- `models`：公共数据结构定义。
- `pkgs`：一些相对独立的功能模块。
  - `cmd`：公用的业务逻辑，比如上传Package和下载Package。
  - `db`：数据库的数据结构和操作函数。
  - `distlock`：分布式锁服务，核心机制使用的是`common/pkgs/distlock`，增加了根据存储系统的业务需求设计的锁。
  - `ec`：纠删码的库。
  - `grpc`：存放proto文件，以及使用protogen工具生成的代码文件。
  - `ioswitch`：IOSwitch模块。
  - `iterator`：迭代器。
  - `mq`：各个服务的rabbitmq接口的声明。
- `utils`：一些暂时没有归类的工具函数。