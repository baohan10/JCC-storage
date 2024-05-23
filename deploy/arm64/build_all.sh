#!/bin/bash

build_folder="$PWD/build"
imagebuild_folder="$PWD/imagebuild"
yml_folder="$PWD/yml"

echo "开始构建agent镜像..."
cd "$imagebuild_folder"/agent || exit
rm -rf agent
rm -rf confs
cp -r "$build_folder"/agent .
cp -r "$build_folder"/confs .
sh build.sh
echo "agent镜像构建完成"

echo "开始构建coordinator镜像..."
cd "$imagebuild_folder"/coordinator || exit
rm -rf coordinator
rm -rf confs
cp -r "$build_folder"/coordinator .
cp -r "$build_folder"/confs .
sh build.sh
echo "coordinator镜像构建完成"

echo "开始构建scanner镜像..."
cd "$imagebuild_folder"/scanner || exit
rm -rf scanner
rm -rf confs
cp -r "$build_folder"/scanner .
cp -r "$build_folder"/confs .
sh build.sh
echo "scanner镜像构建完成"

echo "开始构建client镜像..."
cd "$imagebuild_folder"/client || exit
rm -rf client
rm -rf confs
cp -r "$build_folder"/client .
cp -r "$build_folder"/confs .
sh build.sh
echo "client镜像构建完成"

echo "全部镜像构建完成"
#echo "生成yaml脚本"
#cd "$yml_folder" || exit
#sh replace.sh
