package ec

import (
	"errors"
	"io"
	"io/ioutil"

	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

type BlockReader struct {
	ipfsCli *ipfs.PoolClient
	/*将文件分块相关的属性*/
	//fileHash
	fileHash string
	//fileSize
	fileSize int64
	//ecK将文件的分块数
	ecK int
	//chunkSize
	chunkSize int64

	/*可选项*/
	//fastRead,true的时候直接通过hash读block
	jumpReadOpt bool
}

func NewBlockReader() (*BlockReader, error) {
	ipfsClient, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return nil, err
	}
	//default:fast模式，通过hash直接获取
	return &BlockReader{ipfsCli: ipfsClient, chunkSize: 256 * 1024, jumpReadOpt: false}, nil
}

func (r *BlockReader) Close() {
	r.ipfsCli.Close()
}

func (r *BlockReader) SetJumpRead(fileHash string, fileSize int64, ecK int) {
	r.fileHash = fileHash
	r.fileSize = fileSize
	r.ecK = ecK
	r.jumpReadOpt = true
}

func (r *BlockReader) SetchunkSize(size int64) {
	r.chunkSize = size
}

func (r *BlockReader) FetchBLock(blockHash string) (io.ReadCloser, error) {
	return r.ipfsCli.OpenRead(blockHash)
}

func (r *BlockReader) FetchBLocks(blockHashs []string) ([]io.ReadCloser, error) {
	readers := make([]io.ReadCloser, len(blockHashs))
	for i, hash := range blockHashs {
		var err error
		readers[i], err = r.ipfsCli.OpenRead(hash)
		if err != nil {
			return nil, err
		}
	}
	return readers, nil
}

func (r *BlockReader) JumpFetchBlock(innerID int) (io.ReadCloser, error) {
	if !r.jumpReadOpt {
		return nil, nil
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		for i := int64(r.chunkSize * int64(innerID)); i < r.fileSize; i += int64(r.ecK) * r.chunkSize {
			reader, err := r.ipfsCli.OpenRead(r.fileHash, ipfs.ReadOption{Offset: i, Length: r.chunkSize})
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
			reader.Close()
			_, err = pipeWriter.Write(data)
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
		}
		//如果文件大小不是分块的整数倍,可能需要补0
		if r.fileSize%(r.chunkSize*int64(r.ecK)) != 0 {
			//pktNum_1:chunkNum-1
			pktNum_1 := r.fileSize / (r.chunkSize * int64(r.ecK))
			offset := (r.fileSize - int64(pktNum_1)*int64(r.ecK)*r.chunkSize)
			count0 := int64(innerID)*int64(r.ecK)*r.chunkSize - offset
			if count0 > 0 {
				add0 := make([]byte, count0)
				pipeWriter.Write(add0)
			}
		}
		pipeWriter.Close()
	}()
	return pipeReader, nil
}

// FetchBlock1这个函数废弃了
func (r *BlockReader) FetchBlock1(input interface{}, errMsg chan error) (io.ReadCloser, error) {
	/*两种模式下传入第一个参数，但是input的类型不同：
	jumpReadOpt-》true：传入blcokHash, string型，通过哈希直接读
	jumpReadOpt->false: 传入innerID，int型，选择需要获取的数据块的id
	*/
	var innerID int
	var blockHash string
	switch input.(type) {
	case int:
		// 执行针对整数的逻辑分支
		if r.jumpReadOpt {
			return nil, errors.New("conflict, wrong input type and jumpReadOpt:true")
		} else {
			innerID = input.(int)
		}
	case string:
		if !r.jumpReadOpt {
			return nil, errors.New("conflict, wrong input type and jumpReadOpt:false")
		} else {
			blockHash = input.(string)
		}
	default:
		return nil, errors.New("wrong input type")
	}
	//开始执行
	if r.jumpReadOpt { //快速读
		ipfsCli, err := stgglb.IPFSPool.Acquire()
		if err != nil {
			logger.Warnf("new ipfs client: %s", err.Error())
			return nil, err
		}
		defer ipfsCli.Close()
		return ipfsCli.OpenRead(blockHash)
	} else { //跳跃读
		ipfsCli, err := stgglb.IPFSPool.Acquire()
		if err != nil {
			logger.Warnf("new ipfs client: %s", err.Error())
			return nil, err
		}
		defer ipfsCli.Close()
		pipeReader, pipeWriter := io.Pipe()
		go func() {
			for i := int64(r.chunkSize * int64(innerID)); i < r.fileSize; i += int64(r.ecK) * r.chunkSize {
				reader, err := ipfsCli.OpenRead(r.fileHash, ipfs.ReadOption{i, r.chunkSize})
				if err != nil {
					pipeWriter.Close()
					errMsg <- err
					return
				}
				data, err := ioutil.ReadAll(reader)
				if err != nil {
					pipeWriter.Close()
					errMsg <- err
					return
				}
				reader.Close()
				_, err = pipeWriter.Write(data)
				if err != nil {
					pipeWriter.Close()
					errMsg <- err
					return
				}
			}
			//如果文件大小不是分块的整数倍,可能需要补0
			if r.fileSize%(r.chunkSize*int64(r.ecK)) != 0 {
				//pktNum_1:chunkNum-1
				pktNum_1 := r.fileSize / (r.chunkSize * int64(r.ecK))
				offset := (r.fileSize - int64(pktNum_1)*int64(r.ecK)*r.chunkSize)
				count0 := int64(innerID)*int64(r.ecK)*r.chunkSize - offset
				if count0 > 0 {
					add0 := make([]byte, count0)
					pipeWriter.Write(add0)
				}
			}
			pipeWriter.Close()
			errMsg <- nil
		}()
		return pipeReader, nil
	}
}
