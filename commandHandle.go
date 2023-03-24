package main

import (
	//"context"
	//"log"
	//"os"
	//"io"
	//"fmt"
	//"path/filepath"
	//agentserver "proto"
	"encoding/json"
	"fmt"
	"rabbitmq"
	"sync"
	"time"
	//"google.golang.org/grpc"
)

func commandHandle(wg *sync.WaitGroup) {
	fmt.Println("commandHandle")
	rabbit := rabbitmq.NewRabbitMQSimple("agentQueue" + LocalIp)
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
				var command rabbitmq.RepMoveCommand
				err := json.Unmarshal(c1, &command)
				if err != nil {
					fmt.Printf("json.Unmarshal failed, err:%v\n", err)
				}
				RepMove(command)
			case "01":
				var command rabbitmq.EcMoveCommand
				err := json.Unmarshal(c1, &command)
				if err != nil {
					fmt.Printf("json.Unmarshal failed, err:%v\n", err)
				}
				EcMove(command)
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
	wg.Done()
}
