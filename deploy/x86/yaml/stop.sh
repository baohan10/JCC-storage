#!/bin/bash

# 获取当前路径
current_path=$(pwd)

# 删除configmap
cd $current_path/config
config_files=$(ls *.json 2>/dev/null)

if [ -z "$config_files" ]; then
	echo "当前路径下没有.config.json文件。"
	exit 1
fi

for file in $config_files; do
	if [[ -f "$file" ]]; then
		name=$(echo "$file" | cut -d '.' -f1)
		kubectl delete cm $name-config
	fi
done

# 删除pod
cd $current_path
yaml_files=$(ls *.yaml 2>/dev/null)

for yaml_file in $yaml_files; do
	echo "Delete $yaml_file ..."
	kubectl delete -f $yaml_file
done

# 删除pv
cd $current_path/config
pv_yaml_files=$(ls *.yaml 2>/dev/null)
for pv_yaml_file in $pv_yaml_files; do
	echo "Delete $pv_yaml_file ..."
	kubectl delete -f $pv_yaml_file
done

