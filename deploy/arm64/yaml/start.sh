#!/bin/bash

#!/bin/bash

# 获取当前路径
current_path=$(pwd)

# 拉起configmap
cd $current_path/config
config_files=$(ls *.json 2>/dev/null)

if [ -z "$config_files" ]; then
	echo "当前路径下没有.config.json文件。"
	exit 1
fi

for file in $config_files; do
	if [[ -f "$file" ]]; then
		name=$(echo "$file" | cut -d '.' -f1)
		service_name=$(echo "$name" | cut -d '-' -f1)
		kubectl create cm $name-config --from-file=$service_name.config.json=./$file
	fi
done

# 拉起pod
pv_yaml_files=$(ls rclone_pv_*.yaml 2>/dev/null)
for pv_yaml_file in $pv_yaml_files; do
	echo "Applying $pv_yaml_file ..."
	kubectl apply -f $pv_yaml_file
done

cd $current_path
yaml_files=$(ls *.yaml 2>/dev/null)
for yaml_file in $yaml_files; do
	echo "Applying $yaml_file ..."
	kubectl apply -f $yaml_file
done




