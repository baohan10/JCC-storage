package main
import (
    //"context"
    "log"
    //"os"
    "io"
    "fmt"
    //"path/filepath"
    agentserver "proto"
    "github.com/ipfs/go-ipfs-api"
    "bytes"
    "io/ioutil"
    //"sync"
	//"net"
	//"google.golang.org/grpc"
)

type anyOne struct{
	agentserver.TranBlockOrReplicaServer
}

func UploadIPFS(str string) string {
    var sh *shell.Shell
    sh = shell.NewShell("localhost:5001")

    hash, err := sh.Add(bytes.NewBufferString(str))
    if err != nil {
        fmt.Println("上传ipfs时错误：", err)
    }
    return hash
}

func CatIPFS(hash string) string {
    var sh *shell.Shell
    sh = shell.NewShell("localhost:5001")

    read, err := sh.Cat(hash)
    if err != nil {
        fmt.Println(err)
    }
    body, err := ioutil.ReadAll(read)

    return string(body)
}

func (s *anyOne) SendBlockOrReplica(server agentserver.TranBlockOrReplica_SendBlockOrReplicaServer) error {
    /*fmt.Println("get a request")
        fDir, err := os.Executable()
        if err != nil {
            panic(err)
        }

        fURL := filepath.Join(filepath.Dir(fDir), "assets")
        print(fURL)
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
                    BlockOrReplicaHash: "1235",
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
    */
    i := 0
    var all_data,all_hash string
    var buffer bytes.Buffer //缓存整个块
    var blockOrReplicaName string
    for {
        fmt.Println("get a request")
        data, err := server.Recv()
        if i == 0{
            blockOrReplicaName = data.BlockOrReplicaName
        }
        if i<1 {
            //blockOrReplicaName := data.BlockOrReplicaHash
        }

        if err == io.EOF {
            // 发送结果并关闭(主要发hash)
            all_data = string(buffer.Bytes())
            //上传到ipfs
            all_hash = UploadIPFS(all_data)
            
            return server.SendAndClose(&agentserver.SendRes{
                BlockOrReplicaName: blockOrReplicaName,
                BlockOrReplicaHash: all_hash,
            })
        }
        if err != nil {
            log.Println("err:", err)
            return err
        }
        
        print("!@#!@#@#!@#!@#!#")
        buffer.Write(data.BlockOrReplicaData)
        i++
    }

}

func (s *anyOne) GetBlockOrReplica(req *agentserver.GetReq, server agentserver.TranBlockOrReplica_GetBlockOrReplicaServer) error {
	/*
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
    */
    data := CatIPFS(req.BlockOrReplicaHash)
    
    
    fSize := len(data)
    fmt.Println(fSize)
    numPacket := fSize/packetSizeInBytes
    lastPacketInBytes := fSize%packetSizeInBytes
    if lastPacketInBytes>0 {
        numPacket++
    }

    for i:=0; i<numPacket; i++ {
        var buf []byte
        if i==numPacket-1 && lastPacketInBytes>0 {
            buf = make([]byte, lastPacketInBytes)
        } else {
            buf = make([]byte, packetSizeInBytes)
        } 
        fmt.Println(len(buf))
        buf = []byte(data[i*packetSizeInBytes:i*packetSizeInBytes+packetSizeInBytes])
        fmt.Println(buf)
        print("#@#@#@#@#")
        
        server.Send(&agentserver.BlockOrReplica{
            BlockOrReplicaName: "json",
            BlockOrReplicaHash: "json",
            BlockOrReplicaData: buf,
        })
    }
    return nil
}