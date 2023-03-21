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

func CoorEcWrite(command rabbitmq.EcWriteCommand) {
    
}

func CoorEcWriteHash(command rabbitmq.WriteHashCommand) {

}

func CoorEcRead(command rabbitmq.EcReadCommand) {

}

func CoorRepWrite(command rabbitmq.RepWriteCommand) {
    fmt.Println(command.BucketName)
    
    //返回消息
    res:= rabbitmq.WriteRes{
        Ips: []string{"localhost","localhost","localhost"},
    }
    c2,_:=json.Marshal(res)
    
    queueName := "clientQueue"+strconv.Itoa(command.UserId)
    rabbit := rabbitmq.NewRabbitMQSimple(queueName)
    fmt.Println(string(c2))
    rabbit.PublishSimple(c2)
    rabbit.Destroy()
}

func CoorRepWriteHash(command rabbitmq.WriteHashCommand) {
    fmt.Println(command.BucketName)
    
    //返回消息
    res:= rabbitmq.WriteHashRes{
        MetaCode: 0,
    }
    c2,_:=json.Marshal(res)
    
    queueName := "clientQueue"+strconv.Itoa(command.UserId)
    rabbit := rabbitmq.NewRabbitMQSimple(queueName)
    fmt.Println(string(c2))
    rabbit.PublishSimple(c2)
    rabbit.Destroy()
}

func CoorRepRead(command rabbitmq.RepReadCommand) {

}