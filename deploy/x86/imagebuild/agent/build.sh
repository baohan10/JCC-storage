#!/bin/bash

docker build -t 112.95.163.90:5010/agentservice-x86:latest .
docker push 112.95.163.90:5010/agentservice-x86:latest
