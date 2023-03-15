package main
import (
    //"context"
    "log"
    "os"
    "io"
    "fmt"
    "path/filepath"
    agentserver "proto"
    //"sync"
	//"net"
	//"google.golang.org/grpc"
)

type anyOne struct{
	agentserver.TranBlockOrReplicaServer
}

func (s *anyOne) SendBlockOrReplica(server agentserver.TranBlockOrReplica_SendBlockOrReplicaServer) error {
    fmt.Println("get a request")
    fDir, err := os.Executable()
    if err != nil {
        panic(err)
    }

    fURL := filepath.Join(filepath.Dir(fDir), "assets")
    _, err = os.Stat(fURL)
    if os.IsNotExist(err) {
        log.Println("创建目录")
        os.MkdirAll(fURL, os.ModePerm)
    }
    fmt.Println(filepath.Join(fURL))
    i := 0
    var file *os.File
    for {
        fmt.Println("get a request")
        data, err := server.Recv()
        if i<1 {
            blockOrReplicaName := data.BlockOrReplicaHash
            fmt.Println(filepath.Join(fURL, blockOrReplicaName))
            file, _ = os.Create(filepath.Join(fURL, blockOrReplicaName))
        }

        if err == io.EOF {
			// 发送结果并关闭(主要发hash)
			return server.SendAndClose(&agentserver.SendRes{
                BlockOrReplicaName: "blockname",
                BlockOrReplicaHash: "blockhash",
            })
            file.Close()
		}
        if err != nil {
            log.Println("err:", err)
            file.Close()
            return err
        }
        fmt.Println(data.BlockOrReplicaData)
        file.Write(data.BlockOrReplicaData)
        i++
    }
}

func (s *anyOne) GetBlockOrReplica(req *agentserver.GetReq, server agentserver.TranBlockOrReplica_GetBlockOrReplicaServer) error {
	file, err := os.Open("assets/"+req.BlockOrReplicaHash)
    fmt.Println("assets/"+req.BlockOrReplicaHash)
    if err != nil {
        panic(err)
    }
    fInfo, err := file.Stat()
    if err != nil {
        panic(err)
    }
    
    fSize := fInfo.Size()
    fmt.Println(fSize)
    numPacket := fSize/packetSizeInBytes
    lastPacketInBytes := fSize%packetSizeInBytes
    if lastPacketInBytes>0 {
        numPacket++
    }

    for i:=0; int64(i)<numPacket; i++ {
        var buf []byte
        if int64(i)==numPacket-1 && lastPacketInBytes>0 {
            buf = make([]byte, lastPacketInBytes)
        } else {
            buf = make([]byte, packetSizeInBytes)
        } 
        fmt.Println(len(buf))
        _, err := file.Read(buf)
        if err != nil && err != io.EOF {
            break
        }
        server.Send(&agentserver.BlockOrReplica{
            BlockOrReplicaName: "json",
            BlockOrReplicaHash: "json",
            BlockOrReplicaData: buf,
        })
        if err == io.EOF {
            log.Println(err)
            break
        }
    }
    file.Close()
    return err
}