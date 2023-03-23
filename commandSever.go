package main

import (
    //"context"
    
    "fmt"
    "path/filepath"
    //"sync"
    "encoding/json"
    "strconv"
    "rabbitmq"
    "os"

    //agentcaller "proto"

    //"github.com/pborman/uuid"
    //"github.com/streadway/amqp"

    //"google.golang.org/grpc"

)

func rabbitSend(c []byte, userId int){
    queueName := "agentClientQueue"+strconv.Itoa(userId)
    rabbit := rabbitmq.NewRabbitMQSimple(queueName)
    fmt.Println(string(c))
    rabbit.PublishSimple(c)
    rabbit.Destroy()
}

func RepMove(command rabbitmq.RepMoveCommand) {
    fmt.Println("RepMove")
    fmt.Println(command.Hashs)
    hashs:=command.Hashs
    //执行调度操作
    ipfsDir := "assets"
    goalDir := "assets2"
    goalName := command.BucketName+":"+command.ObjectName+":"+strconv.Itoa(command.UserId)
    //目标文件
    fDir, err := os.Executable()
    if err != nil {
        panic(err)
    }
    fURL := filepath.Join(filepath.Dir(fDir), goalDir)
    
    _, err = os.Stat(fURL)
    if os.IsNotExist(err) {
        os.MkdirAll(fURL, os.ModePerm)
    }
    fURL = filepath.Join(fURL, goalName)
    outFile, err := os.Create(fURL)
    
    fmt.Println(fURL)
    //源文件 
    fURL = filepath.Join(filepath.Dir(fDir), ipfsDir)
    fURL = filepath.Join(fURL, hashs[0])
    inFile, _ := os.Open(fURL)
    fmt.Println(fURL)
    fileInfo, _ := inFile.Stat()
	fileSizeInBytes := fileInfo.Size()
    numWholePacket := fileSizeInBytes/packetSizeInBytes
    lastPacketInBytes:=fileSizeInBytes%packetSizeInBytes
    fmt.Println(fileSizeInBytes)
    fmt.Println(numWholePacket)
    fmt.Println(lastPacketInBytes)
    for i:=0;int64(i)<numWholePacket;i++ {
        buf := make([]byte, packetSizeInBytes)
        inFile.Read(buf)
        outFile.Write(buf)
    }
    if lastPacketInBytes>0 {
        buf := make([]byte, lastPacketInBytes)
        inFile.Read(buf)
        outFile.Write(buf)
    }
    inFile.Close()
    outFile.Close()
    //返回消息
    res:= rabbitmq.AgentMoveRes{
        MoveCode: 0,
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
    //向coor报告临时缓存hash
    command1 := rabbitmq.TempCacheReport{
        Ip : LocalIp,
        Hashs: hashs,
    }
    c,_=json.Marshal(command1)
    b:=append([]byte("07"),c...)
    fmt.Println(b)
    rabbit := rabbitmq.NewRabbitMQSimple("coorQueue")
    rabbit.PublishSimple(b)
    rabbit.Destroy()
}


func EcMove(command rabbitmq.EcMoveCommand) {
    fmt.Println("EcMove")
    fmt.Println(command.Hashs)
    hashs:=command.Hashs

    //执行调度操作

    //返回消息
    res:= rabbitmq.WriteHashRes{
        MetaCode: 0,
    }
    c,_:=json.Marshal(res)
    rabbitSend(c, command.UserId)
    //向coor报告临时缓存hash
    command1:= rabbitmq.TempCacheReport{
        Ip : LocalIp,
        Hashs: hashs,
    }
    c,_=json.Marshal(command1)
    b:=append([]byte("07"),c...)
    fmt.Println(b)
    rabbit := rabbitmq.NewRabbitMQSimple("coorQueue")
    rabbit.PublishSimple(b)
    rabbit.Destroy()
}

