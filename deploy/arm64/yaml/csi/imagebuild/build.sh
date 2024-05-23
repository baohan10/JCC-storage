#!/bin/bash

docker build -t 10.16.43.85:5010/csi-rclone-reloaded_arm64:v1.4.0 .
docker push 10.16.43.85:5010/csi-rclone-reloaded_arm64:v1.4.0
