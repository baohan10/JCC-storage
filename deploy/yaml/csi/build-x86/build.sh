#!/bin/bash

docker build -t 112.95.163.90:5010/csi-rclone-reloaded:v1.4.0 .
docker push 112.95.163.90:5010/csi-rclone-reloaded:v1.4.0
