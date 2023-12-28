# Client服务

## 目录结构
- `internal`：服务源码。
  - `cmdline`：此服务提供的命令行功能。
  - `config`：服务使用的配置文件结构定义。
  - `http`：此服务提供的http接口。
  - `services`：服务的功能，被cmdline和http调用。
  - `task`：需要在后台异步运行的任务。

## 命令行
Client程序可以作为一个命令行程序使用，能在`internal/cmdline`中找到它提供的所有命令。

使用时按照`./client <命令前缀1> <命令前缀2>... <命令函数参数1> <命令函数参数2>...`的方式编写命令。命令前缀在每个文件的init函数中能找到。

以列出某个Bucket下所有Package的命令PackageListBucketPackages为例，它的命令前缀是`pkg ls`，它的函数签名是`PackageListBucketPackages(ctx CommandContext, bucketID cdssdk.BucketID)`，忽略掉会自动填写的ctx参数，需要通过命令行提供的就是bucketID参数，假设为5，因此调用它的命令是：`./client pkg ls 5`。

可以通过使用`serve http`命令将Client程序作为一个http服务启动，并保持运行。