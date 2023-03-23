package main

import (
    //"context"
    //"io"
    "fmt"
    //"path/filepath"
    //"sync"
    "encoding/json"
    "strconv"
    "rabbitmq"

    //agentcaller "proto"

    //"github.com/pborman/uuid"
    //"github.com/streadway/amqp"

    //"google.golang.org/grpc"

)

func rabbitSend(c []byte, userId int){
    queueName := "coorClientQueue"+strconv.Itoa(userId)
    rabbit := rabbitmq.NewRabbitMQSimple(queueName)
    fmt.Println(string(c))
    rabbit.PublishSimple(c)
    rabbit.Destroy()
}


func TempCacheReport(command rabbitmq.TempCacheReport) {
    fmt.Println("TempCacheReport")
    fmt.Println(command.Hashs)
    fmt.Println(command.Ip)
    //返回消息
    
}

func CoorMove(command rabbitmq.MoveCommand) {
    fmt.Println("CoorMove")
    fmt.Println(command.BucketName)
    //查询数据库，获取冗余类型，冗余参数
    redundancy := "rep"
    ecName := "ecName"
    hashs := []string{"block1.json","block2.json"}
    ids := []int{0,1}
    fileSizeInBytes :=21
    
    res:= rabbitmq.MoveRes{
        Redundancy: redundancy,
        EcName : ecName,
        Hashs : hashs,
        Ids : ids,
        FileSizeInBytes: int64(fileSizeInBytes),
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
}


func CoorEcWrite(command rabbitmq.EcWriteCommand) {
    fmt.Println("CoorEcWrite")
    fmt.Println(command.BucketName)
    //返回消息
    res:= rabbitmq.WriteRes{
        Ips: []string{"localhost","localhost","localhost"},
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
    
}

func CoorEcWriteHash(command rabbitmq.WriteHashCommand) {
    fmt.Println("CoorEcWriteHash")
    fmt.Println(command.BucketName)
    
    //返回消息
    res:= rabbitmq.WriteHashRes{
        MetaCode: 0,
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
}

func CoorRead(command rabbitmq.ReadCommand) {
    fmt.Println("CoorRead")
    fmt.Println(command.BucketName)
    
    //返回消息
    res:= rabbitmq.ReadRes{
        Redundancy: "rep",
        Ips: []string{"localhost","localhost"},
	    Hashs: []string{"block1.json","block2.json"},
	    BlockIds: []int{0,1}, 
	    EcName: "ecName",
	    FileSizeInBytes: 21,
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
}

func CoorRepWriteHash(command rabbitmq.WriteHashCommand) {
    fmt.Println("CoorRepWriteHash")
    fmt.Println(command.BucketName)
    
    //返回消息
    res:= rabbitmq.WriteHashRes{
        MetaCode: 0,
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
}

func CoorRepWrite(command rabbitmq.RepWriteCommand) {
    fmt.Println("CoorRepWrite")
    fmt.Println(command.BucketName)
    
    //返回消息
    res:= rabbitmq.WriteRes{
        Ips: []string{"localhost","localhost","localhost"},
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
}