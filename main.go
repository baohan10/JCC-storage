package main

import (
    //"context"
    //"io"
    "fmt"
    //"path/filepath"
    //"sync"
    "rabbitmq"
    "time"
    "encoding/json"
    //agentcaller "proto"

    //"github.com/pborman/uuid"
    //"github.com/streadway/amqp"

    //"google.golang.org/grpc"

)

func main() {
    rabbit := rabbitmq.NewRabbitMQSimple("coorQueue")
    msgs:=rabbit.ConsumeSimpleQos(time.Millisecond * 500)
    forever := make(chan bool)
    // 启用协程处理消息
    go func() {
        for d := range msgs {
            // 实现我们要处理的逻辑函数
            b1:=d.Body
            commandType := string(b1[0:2])
            fmt.Println(commandType)
            c1:=b1[2:]
            switch commandType {
                case "00":
                    var command rabbitmq.EcWriteCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorEcWrite(command) 
                case "01":
                    var command rabbitmq.WriteHashCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorEcWriteHash(command) 
                case "02":
                    var command rabbitmq.EcReadCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorEcRead(command) 
                case "03":
                    var command rabbitmq.RepWriteCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorRepWrite(command) 
                case "04":
                    var command rabbitmq.WriteHashCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorRepWriteHash(command) 
                case "05":
                    var command rabbitmq.RepReadCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorRepRead(command) 
                case "06":
                    var command rabbitmq.MoveCommand 
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    CoorMove(command) 
                case "07":
                    var command rabbitmq.TempCacheReport
	                err := json.Unmarshal(c1, &command)
	                if err != nil {
		                fmt.Printf("json.Unmarshal failed, err:%v\n", err)
	                }
                    TempCacheReport(command)
            }
            //log.Printf("Received a message: %s", d.Body)
            //time.Sleep(duration)
            //如果为true表示确认所有未确认的消息
            //如果为false表示确认当前消息
            //执行完业务逻辑成功之后我们再手动ack告诉服务器你可以删除这个消息啦！ 这样就保障了数据的绝对的安全不丢失！
            err := d.Ack(false)
            if err != nil {
                println(err)
            }
        }
    }()
    //log.Printf("[*] waiting for messages, [退出请按]To exit press CTRL+C")
    <-forever

}