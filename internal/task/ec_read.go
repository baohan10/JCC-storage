package task

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-agent/internal/config"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/ec"
)

type EcRead struct {
	objID      int64
	FileSize   int64
	Ec         models.EC
	BlockIDs   []int
	BlockHashs []string
	LocalPath  string
}

func NewEcRead(objID int64, fileSize int64, ec models.EC, blockIDs []int, blockHashs []string, localPath string) *EcRead {
	return &EcRead{
		objID:      objID,
		FileSize:   fileSize,
		Ec:         ec,
		BlockIDs:   blockIDs,
		BlockHashs: blockHashs,
		LocalPath:  localPath,
	}
}

func (t *EcRead) Compare(other *Task) bool {
	tsk, ok := other.Body().(*EcRead)
	if !ok {
		return false
	}

	return t.objID == tsk.objID && t.LocalPath == tsk.LocalPath
}

func (t *EcRead) Execute(ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[EcRead]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	outputFileDir := filepath.Dir(t.LocalPath)

	err := os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		err := fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	outputFile, err := os.Create(t.LocalPath)
	if err != nil {
		err := fmt.Errorf("create output file %s failed, err: %w", t.LocalPath, err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer outputFile.Close()
	//这里引入ecread
	ecK := t.Ec.EcK
	ecN := t.Ec.EcN
	rd, err := readObject(ctx, t.FileSize, ecK, ecN, t.BlockIDs, t.BlockHashs)
	if err != nil {
		err := fmt.Errorf("read ipfs block failed, err: %w", err)
		log.WithField("FileHash1", t.BlockHashs[0]).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	_, err = io.Copy(outputFile, rd)
	if err != nil {
		err := fmt.Errorf("copy ipfs file to local file failed, err: %w", err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func readObject(ctx TaskContext, fileSize int64, ecK int, ecN int, blockIDs []int, hashs []string) (io.ReadCloser, error) {
	// TODO zkx 先使用同步方式实现读取多个block并解码数据的逻辑，做好错误处理

	numPacket := (fileSize + int64(ecK)*config.Cfg().ECPacketSize - 1) / (int64(ecK) * config.Cfg().ECPacketSize)
	getBufs := make([]chan []byte, ecN)
	decodeBufs := make([]chan []byte, ecK)
	for i := 0; i < ecN; i++ {
		getBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecK; i++ {
		decodeBufs[i] = make(chan []byte)
	}
	for i := 0; i < len(blockIDs); i++ {
		go get(ctx, hashs[i], getBufs[blockIDs[i]], numPacket)
	}
	//print(numPacket)
	go decode(getBufs[:], decodeBufs[:], blockIDs, ecK, numPacket)
	r, w := io.Pipe()
	//这个就是persist函数
	go func() {
		for i := 0; int64(i) < numPacket; i++ {
			for j := 0; j < len(decodeBufs); j++ {
				tmp := <-decodeBufs[j]
				_, err := w.Write(tmp)
				if err != nil {
					fmt.Errorf("persist file falied, err:%w", err)
				}
			}
		}
		w.Close()
	}()
	return r, nil
}

func get(ctx TaskContext, blockHash string, getBuf chan []byte, numPacket int64) error {
	//使用本地IPFS获取
	//获取IPFS的reader
	reader, err := ctx.IPFS.OpenRead(blockHash)
	if err != nil {
		return fmt.Errorf("read ipfs block failed, err: %w", err)
	}
	defer reader.Close()
	for i := 0; int64(i) < numPacket; i++ {
		buf := make([]byte, config.Cfg().ECPacketSize)
		_, err := io.ReadFull(reader, buf)
		if err != nil {
			return fmt.Errorf("read file falied, err:%w", err)
		}
		getBuf <- buf
	}
	close(getBuf)
	return nil
}

func decode(inBufs []chan []byte, outBufs []chan []byte, blockSeq []int, ecK int, numPacket int64) {
	var tmpIn [][]byte
	var zeroPkt []byte
	tmpIn = make([][]byte, len(inBufs))
	hasBlock := map[int]bool{}
	for j := 0; j < len(blockSeq); j++ {
		hasBlock[blockSeq[j]] = true
	}
	needRepair := false //检测是否传入了所有数据块
	for j := 0; j < len(outBufs); j++ {
		if blockSeq[j] != j {
			needRepair = true
		}
	}
	enc := ec.NewRsEnc(ecK, len(inBufs))
	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(inBufs); j++ { //3
			if hasBlock[j] {
				tmpIn[j] = <-inBufs[j]
			} else {
				tmpIn[j] = zeroPkt
			}
		}
		if needRepair {
			err := enc.Repair(tmpIn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Decode Repair Error: %s", err.Error())
			}
		}
		for j := 0; j < len(outBufs); j++ { //1,2,3//示意，需要调用纠删码编解码引擎：  tmp[k] = tmp[k]+(tmpIn[w][k]*coefs[w][j])
			outBufs[j] <- tmpIn[j]
		}
	}
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}
