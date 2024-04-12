package downloader

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/plans"
)

type IPFSReader struct {
	node     cdssdk.Node
	fileHash string
	stream   io.ReadCloser
	offset   int64
}

func NewIPFSReader(node cdssdk.Node, fileHash string) *IPFSReader {
	return &IPFSReader{
		node:     node,
		fileHash: fileHash,
	}
}

func NewIPFSReaderWithRange(node cdssdk.Node, fileHash string, rng ipfs.ReadOption) io.ReadCloser {
	str := &IPFSReader{
		node:     node,
		fileHash: fileHash,
	}
	str.Seek(rng.Offset, io.SeekStart)
	if rng.Length > 0 {
		return io2.Length(str, rng.Length)
	}

	return str
}

func (r *IPFSReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekEnd {
		return 0, fmt.Errorf("seek end not supported")
	}

	if whence == io.SeekCurrent {
		return 0, fmt.Errorf("seek current not supported")
	}

	if r.stream == nil {
		r.offset = offset
		return r.offset, nil
	}

	// 如果文件流已经打开，那么如果seek的位置和当前位置不同，那么需要重新打开文件流
	if offset != r.offset {
		var err error
		r.stream.Close()
		r.offset = offset
		r.stream, err = r.openStream()
		if err != nil {
			return 0, fmt.Errorf("reopen stream: %w", err)
		}
	}

	return r.offset, nil
}

func (r *IPFSReader) Read(buf []byte) (int, error) {
	if r.stream == nil {
		var err error
		r.stream, err = r.openStream()
		if err != nil {
			return 0, err
		}
	}

	n, err := r.stream.Read(buf)
	r.offset += int64(n)
	return n, err
}

func (r *IPFSReader) Close() error {
	if r.stream != nil {
		return r.stream.Close()
	}

	return nil
}

func (r *IPFSReader) openStream() (io.ReadCloser, error) {
	if stgglb.IPFSPool != nil {
		logger.Infof("try to use local IPFS to download file")

		reader, err := r.fromLocalIPFS()
		if err == nil {
			return reader, nil
		}

		logger.Warnf("download from local IPFS failed, so try to download from node %v, err: %s", r.node.Name, err.Error())
	}

	return r.fromNode()
}

func (r *IPFSReader) fromNode() (io.ReadCloser, error) {
	planBld := plans.NewPlanBuilder()
	fileStr := planBld.AtAgent(r.node).IPFSRead(r.fileHash, ipfs.ReadOption{
		Offset: r.offset,
		Length: -1,
	}).ToExecutor()

	plan, err := planBld.Build()
	if err != nil {
		return nil, fmt.Errorf("building plan: %w", err)
	}

	waiter, err := plans.Execute(*plan)
	if err != nil {
		return nil, fmt.Errorf("execute plan: %w", err)
	}
	go func() {
		waiter.Wait()
	}()

	return waiter.ReadStream(fileStr)
}

func (r *IPFSReader) fromLocalIPFS() (io.ReadCloser, error) {
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new ipfs client: %w", err)
	}

	reader, err := ipfsCli.OpenRead(r.fileHash, ipfs.ReadOption{
		Offset: r.offset,
		Length: -1,
	})
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	reader = io2.AfterReadClosed(reader, func(io.ReadCloser) {
		ipfsCli.Close()
	})
	return reader, nil
}
