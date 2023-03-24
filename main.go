package main

import (
	//"context"
	//"io"
	"fmt"
	"os"

	//"path/filepath"
	//"sync"
	"strconv"
	//agentcaller "proto"

	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"

	//"google.golang.org/grpc"

	_ "google.golang.org/grpc/balancer/grpclb"
)

func main() {
	args := os.Args
	arg_num := len(os.Args)
	for i := 0; i < arg_num; i++ {
		fmt.Println(args[i])
	}

	switch args[1] {
	case "ecWrite":
		EcWrite(args[2], args[3], args[4], args[5])
	case "write":
		numRep, _ := strconv.Atoi(args[5])
		RepWrite(args[2], args[3], args[4], numRep)
	case "read":
		Read(args[2], args[3], args[4])
	case "move":
		Move(args[2], args[3], args[4]) //bucket object destination
	}
}
