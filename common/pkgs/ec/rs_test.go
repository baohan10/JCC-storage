package ec

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	//"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	//"gitlink.org.cn/cloudream/storage/agent/internal/config"
	//stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

func test_Encode(t *testing.T) {
	enc, _ := NewRs(3, 5, 10)
	rc := make([]io.ReadCloser, 3)
	rc[0] = ioutil.NopCloser(bytes.NewBufferString("11111111"))
	rc[1] = ioutil.NopCloser(bytes.NewBufferString("22222222"))
	rc[2] = ioutil.NopCloser(bytes.NewBufferString("33333333"))
	/*rc[0].Close()
	rc[1].Close()
	rc[2].Close()*/
	print("#$$$$$$$$$$$")
	out, _ := enc.ReconstructData(rc, []int{0, 1, 2})
	//out, _ := enc.Encode(rc)
	buf := make([]byte, 100)
	out[0].Read(buf)
	fmt.Println(buf)
	out[1].Read(buf)
	fmt.Println(buf)
	t.Logf(string(buf))
	t.Log(buf)
}

/*
------------------------------------------------
hash:QmX49sGugmtVPfNo13q84YL1NwGmr5yzWDDmJZ7PniQ9b6
内容：1111122222233333333334444444445663454543534534

hash:QmcN1EJm2w9XT62Q9YqA5Ym7YDzjmnqJYc565bzRs5VosW
(5,3),chunkSize:6
data1:QmS2t7xFgTMTX2DGYsbDdmHnGvaG6sc7D9k1R2WZyuDx56
data2:QmUSZvuABjfGKF1c4VxvVBdH31SroDm2QyLGBrVFomRM8P
data3:QmcD3RpUh5rwMhf9yBywBeT6ibT1P5DSJC67aoD77jhTBn
内容：qqqqqqqqwwwwwwwwwwwwwweeeeeeeeeeeeerrrrrrrrrrr
-----------------------------------------------------
*/
func test_Fetch(t *testing.T) {

	blkReader, _ := NewBlockReader()
	/*****************************FetchBlock*************************/
	/*r, _ := blkReader.FetchBLock("QmX49sGugmtVPfNo13q84YL1NwGmr5yzWDDmJZ7PniQ9b6")
	data, _ := ioutil.ReadAll(r)
	t.Logf(string(data))*/

	/**********************FetchBlocks************************************
	hashs := []string{"QmcN1EJm2w9XT62Q9YqA5Ym7YDzjmnqJYc565bzRs5VosW", "QmX49sGugmtVPfNo13q84YL1NwGmr5yzWDDmJZ7PniQ9b6"}
	rs, _ := blkReader.FetchBLocks(hashs)
	data1, _ := ioutil.ReadAll(rs[0])
	data2, _ := ioutil.ReadAll(rs[1])
	t.Logf(string(data1))
	t.Logf(string(data2))
	/*************************JumpFetchBlock*********************************/
	blkReader.SetJumpRead("QmcN1EJm2w9XT62Q9YqA5Ym7YDzjmnqJYc565bzRs5VosW", 46, 3)
	blkReader.SetchunkSize(6)
	r, _ := blkReader.JumpFetchBlock(1)
	data, _ := ioutil.ReadAll(r)
	t.Logf(string(data))
}
func test_Fetch_and_Encode(t *testing.T) {
	chunkSize := int64(6)
	blkReader, _ := NewBlockReader()
	defer blkReader.Close()
	blkReader.SetJumpRead("QmcN1EJm2w9XT62Q9YqA5Ym7YDzjmnqJYc565bzRs5VosW", 46, 3)
	blkReader.SetchunkSize(int64(chunkSize))
	dataBlocks := make([]io.ReadCloser, 3)
	for i := range dataBlocks {
		dataBlocks[i], _ = blkReader.JumpFetchBlock(i)
	}
	enc, _ := NewRs(3, 5, chunkSize)
	parityBlocks, _ := enc.Encode(dataBlocks)

	parityData := make([]string, 2)
	finished := false
	for {
		if finished {
			break
		}
		buf := make([]byte, chunkSize)
		for i, pipe := range parityBlocks {
			_, err := pipe.Read(buf)
			if err != nil {
				finished = true
				break
			}
			parityData[i] = parityData[i] + string(buf)
		}
	}
	t.Logf(parityData[0])
	t.Logf(parityData[1])

}

func test_Fetch_and_Encode_and_Degraded(t *testing.T) {
	chunkSize := int64(6)
	blkReader, _ := NewBlockReader()
	defer blkReader.Close()
	blkReader.SetJumpRead("QmcN1EJm2w9XT62Q9YqA5Ym7YDzjmnqJYc565bzRs5VosW", 46, 3)
	blkReader.SetchunkSize(int64(chunkSize))
	dataBlocks := make([]io.ReadCloser, 3)
	for i := range dataBlocks {
		dataBlocks[i], _ = blkReader.JumpFetchBlock(i)
	}
	enc, _ := NewRs(3, 5, chunkSize)
	parityBlocks, _ := enc.Encode(dataBlocks)
	go func() {
		ioutil.ReadAll(parityBlocks[0])
	}()
	degradedBlocks := make([]io.ReadCloser, 3)
	degradedBlocks[0], _ = blkReader.JumpFetchBlock(1)
	degradedBlocks[1], _ = blkReader.JumpFetchBlock(2)
	degradedBlocks[2] = parityBlocks[1]
	newDataBlocks, _ := enc.ReconstructData(degradedBlocks, []int{1, 2, 4})
	newData := make([]string, 3)
	finished := false
	for {
		if finished {
			break
		}
		buf := make([]byte, chunkSize)
		for i, pipe := range newDataBlocks {
			_, err := pipe.Read(buf)
			if err != nil {
				finished = true
				break
			}
			newData[i] = newData[i] + string(buf)
		}
	}
	t.Logf(newData[0])
	t.Logf(newData[1])
	t.Logf(newData[2])

}

func test_pin_data_blocks(t *testing.T) {
	chunkSize := int64(6)
	blkReader, _ := NewBlockReader()
	defer blkReader.Close()
	blkReader.SetJumpRead("QmcN1EJm2w9XT62Q9YqA5Ym7YDzjmnqJYc565bzRs5VosW", 46, 3)
	blkReader.SetchunkSize(int64(chunkSize))
	dataBlocks := make([]io.ReadCloser, 3)
	ipfsclient, _ := stgglb.IPFSPool.Acquire()
	for i := range dataBlocks {
		dataBlocks[i], _ = blkReader.JumpFetchBlock(i)
		hash, _ := ipfsclient.CreateFile(dataBlocks[i])
		t.Logf(hash)
	}

}

func print_ioreaders(t *testing.T, readers []io.ReadCloser, chunkSize int64) {
	newData := make([]string, len(readers))
	finished := false
	for {
		if finished {
			break
		}
		buf := make([]byte, chunkSize)
		for i, pipe := range readers {
			_, err := pipe.Read(buf)
			if err != nil {
				finished = true
				break
			}
			newData[i] = newData[i] + string(buf)
		}
	}
	for _, data := range newData {
		t.Logf(data)
	}
}

func test_reconstructData(t *testing.T) {
	blkReader, _ := NewBlockReader()
	defer blkReader.Close()
	hashs := []string{"QmS2t7xFgTMTX2DGYsbDdmHnGvaG6sc7D9k1R2WZyuDx56", "QmUSZvuABjfGKF1c4VxvVBdH31SroDm2QyLGBrVFomRM8P", "QmcD3RpUh5rwMhf9yBywBeT6ibT1P5DSJC67aoD77jhTBn"}
	dataBlocks, _ := blkReader.FetchBLocks(hashs)
	chunkSize := int64(6)
	enc, _ := NewRs(3, 5, chunkSize)
	print("@@@@@@@@@")
	newDataBlocks, _ := enc.ReconstructSome(dataBlocks, []int{0, 1, 2}, []int{3, 4})
	print("!!!!!!!!!")
	print_ioreaders(t, newDataBlocks, chunkSize)
}
func Test_main(t *testing.T) {
	//test_Encode(t)
	//stgglb.InitLocal(&config.Cfg().Local)
	stgglb.InitIPFSPool(&ipfs.Config{Port: 5001})
	//test_Fetch(t)
	//test_Fetch_and_Encode(t)
	//test_Fetch_and_Encode_and_Degraded(t)
	//test_pin_data_blocks(t)
	test_reconstructData(t)
}

/*
func Test_Fetch_Encode_ReconstructData(t *testing.T) {
	inFileName := "test.txt"
	enc, _ := NewRs(3, 5, 10)
	file, err := os.Open(inFileName)
	if err != nil {
		t.Error(err)
	}
	var data io.ReadCloser
	data = file
	//enc.Encode(data)
}*/
