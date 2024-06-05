#!/bin/bash

# 配置文件路径
config_file="config.ini"

# YAML 模板文件路径
agent_template_file="template/rclone-agent.yaml.template"
rclone_pv_template_file="template/rclone-pv.yaml.template"
coordinator_template_file="template/coordinator.yaml.template"
scanner_template_file="template/scanner.yaml.template"
client_template_file="template/client.yaml.template"


image_registry_address=$(awk -F "=" '/\[General\]/{f=1} f==1 && /image_registry/{print $2; exit}' "$config_file")

# 逐行读取文件
while IFS= read -r line; do
	# 判断是否是标题行
	if [[ $line == \[*] ]]; then
		current_section=$(echo "$line" | tr -d '[]')
        
		case $current_section in
			agent*)
				echo "读取agent配置"
				while IFS= read -r next_line && [[ $next_line ]]; do
					case "$next_line" in
						label=*) label=$(echo "$next_line" | cut -d '=' -f 2) ;;
						port=*) port=$(echo "$next_line" | cut -d '=' -f 2) ;;
						node=*) node=$(echo "$next_line" | cut -d '=' -f 2) ;;
					esac
				done
				if [[ -n $label && -n $port && -n $node ]]; then
					# 创建并替换 YAML 文件
					agent_yaml_file="rclone_agent_$label.yaml"
					#pv_yaml_file="rclone_pv_$label.yaml"
					
					if [ ! -f "$agent_yaml_file" ]; then
						sed "s/{{NODE_NAME}}/$label/g; 
							 s/{{NODE_PORT}}/$port/g;
							 s/{{NODE_ID}}/$node/g;
							 s/{{IMAGE_REGISTRY_ADDRESS}}/$image_registry_address/g" "$agent_template_file" > "$agent_yaml_file"
					fi
					
					#if [ ! -f "$pv_yaml_file" ]; then
					#	sed "s/{{NODE_NAME}}/$label/g" "$rclone_pv_template_file" > "$pv_yaml_file"
					#fi
					
					# 清空变量，以便下一个节点的处理
					label=""
					port=""
					node=""
				fi
				;;

			coordinator)
				echo "读取coordinator配置"
				while IFS= read -r next_line && [[ $next_line ]]; do
					case "$next_line" in
						label=*) coor_label=$(echo "$next_line" | cut -d '=' -f 2) ;;
						node=*) coor_node=$(echo "$next_line" | cut -d '=' -f 2) ;;
					esac
				done
				if [[ -n $coor_label && -n $coor_node ]]; then
					coor_yaml_file="coordinator.yaml"
					if [ ! -f "$coor_yaml_file" ]; then
						sed "s/{{NODE_NAME}}/$coor_label/g;
							 s/{{IMAGE_REGISTRY_ADDRESS}}/$image_registry_address/g" "$coordinator_template_file" > "$coor_yaml_file"
					fi
				fi
				;;
	
			scanner)
				echo "读取scanner配置"
				while IFS= read -r next_line && [[ $next_line ]]; do
					case "$next_line" in
						label=*) scan_label=$(echo "$next_line" | cut -d '=' -f 2) ;;
						node=*) scan_node=$(echo "$next_line" | cut -d '=' -f 2) ;;
					esac
				done
				if [[ -n $scan_label && -n $scan_node ]]; then
					scan_yaml_file="scanner.yaml"
					if [ ! -f "$scan_yaml_file" ]; then
						sed "s/{{NODE_NAME}}/$scan_label/g;
							 s/{{IMAGE_REGISTRY_ADDRESS}}/$image_registry_address/g" "$scanner_template_file" > "$scan_yaml_file"
					fi
				fi
				;;

			client)
				echo "读取client配置"
				while IFS= read -r next_line && [[ $next_line ]]; do
					case "$next_line" in                
						label=*) cli_label=$(echo "$next_line" | cut -d '=' -f 2) ;;
						port=*) cli_port=$(echo "$next_line" | cut -d '=' -f 2) ;;
						node=*) cli_node=$(echo "$next_line" | cut -d '=' -f 2) ;;
					esac
				done
				if [[ -n $cli_label && -n $cli_port && -n $cli_node ]]; then
					cli_yaml_file="client.yaml"
					if [ ! -f "$cli_yaml_file" ]; then
						sed "s/{{NODE_NAME}}/$cli_label/g;
							 s/{{NODE_PORT}}/$cli_port/g;
							 s/{{IMAGE_REGISTRY_ADDRESS}}/$image_registry_address/g" "$client_template_file" > "$cli_yaml_file"
					fi
				fi
				;;
		esac
	fi
done < "$config_file"
