package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type IPFSRead struct {
	Output   *ioswitch.StreamVar `json:"output"`
	FileHash string              `json:"fileHash"`
	Option   ipfs.ReadOption     `json:"option"`
}

func (o *IPFSRead) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	logger.
		WithField("FileHash", o.FileHash).
		WithField("Output", o.Output).
		Debugf("ipfs read op")
	defer logger.Debugf("ipfs read op finished")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	file, err := ipfsCli.OpenRead(o.FileHash, o.Option)
	if err != nil {
		return fmt.Errorf("reading ipfs: %w", err)
	}
	defer file.Close()

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosedOnce(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Output)

	return fut.Wait(ctx)
}

type IPFSWrite struct {
	Input    *ioswitch.StreamVar `json:"input"`
	FileHash *ioswitch.StringVar `json:"fileHash"`
}

func (o *IPFSWrite) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	logger.
		WithField("ResultKey", o.FileHash).
		WithField("Input", o.Input).
		Debugf("ipfs write op")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	err = sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	o.FileHash.Value, err = ipfsCli.CreateFile(o.Input.Stream)
	if err != nil {
		return fmt.Errorf("creating ipfs file: %w", err)
	}

	sw.PutVars(o.FileHash)

	return nil
}

func init() {
	OpUnion.AddT((*IPFSRead)(nil))
	OpUnion.AddT((*IPFSWrite)(nil))
}
