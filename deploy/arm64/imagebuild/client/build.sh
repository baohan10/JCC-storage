#!/bin/bash

docker build -t 10.16.43.85:5010/clientservice-arm64:latest .
docker push 10.16.43.85:5010/clientservice-arm64:latest
