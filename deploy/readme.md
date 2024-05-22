1、将storage编译好的build文件放在cloudream目录下
2、在cloudream目录下，运行build_all.sh，构建镜像
./build_all.sh
3、进入yaml目录
cd yaml
4、修改config.ini，填好镜像仓库及其他agent部署信息
5、运行replace.sh，生成yaml文件
6、修改rclone_pv_*.yaml，填写对应节点的rclone配置文件
7、执行启动脚本start.sh，运行pod
./start.sh
8、等待启动完成后，查看pod是否正常运行
kubectl get po -A -owide
9、若需要停止，执行stop.sh
./stop.sh
