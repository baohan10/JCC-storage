package main

import (
    //"context"
//    "log"
//    "os"
//    "io"
//    "fmt"
//    "path/filepath"
    agentserver "proto"
    "sync"
	"net"
	"google.golang.org/grpc"
)

const (
    Port = ":5000"
    packetSizeInBytes=10
)


func main(){
    //面向客户端收发数据
    lis, err := net.Listen("tcp", Port)
    if err != nil {
        panic(err)
    }
    s := grpc.NewServer()
    agentserver.RegisterTranBlockOrReplicaServer(s, &anyOne{})
    s.Serve(lis)
    //处置协调端、客户端命令（可多建几个）
    wg := sync.WaitGroup{}
    wg.Add(1)
    go commandHandle(&wg)
    wg.Wait()
    //
}