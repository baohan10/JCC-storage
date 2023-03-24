package main

import (
	//"context"
	//"io"
	"fmt"
	//"path/filepath"
	//"sync"
	"encoding/json"
	"rabbitmq"
	"time"
	//agentcaller "proto"
	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"
	//"google.golang.org/grpc"
)

func main() {
	rabbit := rabbitmq.NewRabbitMQSimple("coorQueue")
	msgs := rabbit.ConsumeSimpleQos(time.Millisecond * 500)
	forever := make(chan bool)
	// 启用协程处理消息
	go func() {
		for d := range msgs {
			// 实现我们要处理的逻辑函数
			b1 := d.Body
			commandType := string(b1[0:2])
			fmt.Println(commandType)
			c1 := b1[2:]
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
				var command rabbitmq.ReadCommand
				err := json.Unmarshal(c1, &command)
				if err != nil {
					fmt.Printf("json.Unmarshal failed, err:%v\n", err)
				}
				CoorRead(command)
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
				var command rabbitmq.MoveCommand
				err := json.Unmarshal(c1, &command)
				if err != nil {
					fmt.Printf("json.Unmarshal failed, err:%v\n", err)
				}
				CoorMove(command)
			case "06":
				var command rabbitmq.TempCacheReport
				err := json.Unmarshal(c1, &command)
				if err != nil {
					fmt.Printf("json.Unmarshal failed, err:%v\n", err)
				}
				TempCacheReport(command)
			case "07":
				var command rabbitmq.HeartReport
				err := json.Unmarshal(c1, &command)
				if err != nil {
					fmt.Printf("json.Unmarshal failed, err:%v\n", err)
				}
				HeartReport(command)
			}
			err := d.Ack(false)
			if err != nil {
				println(err)
			}
		}
	}()
	<-forever

}
