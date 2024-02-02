# 跨算力中心存储系统


## 目录结构
此仓库是一个go module，但包含了多个服务的源码，你可以在每个服务的目录中找到main.go。可以通过编译脚本的参数来指定生成哪一个服务。

- `agent`：Agent服务的源码。
- `client`：Client服务的源码。
- `common`：存放在几个服务之间共享的代码以及一些数据结构定义。
- `coordinator`：Coordinator服务的源码。
- `scanner`：Scanner服务的源码。

同时还有以下两个与编译相关的目录：
- `build`：服务编译后的输出目录，只会在编译后生成。
- `magefiles`：mage工具使用的编译脚本。

## 编译
运行编译脚本需要使用mage工具，此处是[仓库链接](https://github.com/magefile/mage)。

安装好mage工具之后，进入到仓库根目录，使用`mage agent`即可编译Agent服务。与此相同的还有`mage client`、`mage coodinator`、`mage scanner`。可以同时指定多个参数来编译多个服务，如果要一次性编译所有服务，可以使用`mage bin`。

使用`mage confs`命令可以将`common/assets/confs`的配置文件拷贝到输出目录，使用`mage scripts`将`scripts`目录里的脚本拷贝到输出目录。

使用`mage all`可以一次性完成编译、拷贝工作。

可以通过增加额外的参数来指定编译目标平台，比如`mage win amd64 agent`。支持的操作系统参数有`win`、`linux`，支持的CPU架构参数有`amd64`、`arm64`。这些参数同样可以和`bin`、`all`参数一起使用。

注意：编译目标平台参数必须在编译二进制参数之前。
