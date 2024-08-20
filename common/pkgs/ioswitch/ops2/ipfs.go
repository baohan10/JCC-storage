package ops2

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

func init() {
	exec.UseOp[*IPFSRead]()
	exec.UseOp[*IPFSWrite]()
}

type IPFSRead struct {
	Output   *exec.StreamVar `json:"output"`
	FileHash string          `json:"fileHash"`
	Option   ipfs.ReadOption `json:"option"`
}

func (o *IPFSRead) Execute(ctx context.Context, e *exec.Executor) error {
	logger.
		WithField("FileHash", o.FileHash).
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
	e.PutVars(o.Output)

	return fut.Wait(ctx)
}

type IPFSWrite struct {
	Input    *exec.StreamVar `json:"input"`
	FileHash *exec.StringVar `json:"fileHash"`
}

func (o *IPFSWrite) Execute(ctx context.Context, e *exec.Executor) error {
	logger.
		WithField("Input", o.Input.ID).
		WithField("FileHashVar", o.FileHash.ID).
		Debugf("ipfs write op")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	err = e.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	o.FileHash.Value, err = ipfsCli.CreateFile(o.Input.Stream)
	if err != nil {
		return fmt.Errorf("creating ipfs file: %w", err)
	}

	e.PutVars(o.FileHash)

	return nil
}

type IPFSReadType struct {
	FileHash string
	Option   ipfs.ReadOption
}

func (t *IPFSReadType) InitNode(node *dag.Node) {
	dag.NodeNewOutputStream(node, &ioswitch.VarProps{})
}

func (t *IPFSReadType) GenerateOp(n *dag.Node) (exec.Op, error) {
	return &IPFSRead{
		Output:   n.OutputStreams[0].Var,
		FileHash: t.FileHash,
		Option:   t.Option,
	}, nil
}

func (t *IPFSReadType) String(node *dag.Node) string {
	return fmt.Sprintf("IPFSRead[%s,%v+%v]%v%v", t.FileHash, t.Option.Offset, t.Option.Length, formatStreamIO(node), formatValueIO(node))
}

type IPFSWriteType struct {
	FileHashStoreKey string
	Range            exec.Range
}

func (t *IPFSWriteType) InitNode(node *dag.Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputValue(node, &ioswitch.VarProps{})
}

func (t *IPFSWriteType) GenerateOp(op *dag.Node) (exec.Op, error) {
	return &IPFSWrite{
		Input:    op.InputStreams[0].Var,
		FileHash: op.OutputValues[0].Var.(*exec.StringVar),
	}, nil
}

func (t *IPFSWriteType) String(node *dag.Node) string {
	return fmt.Sprintf("IPFSWrite[%s,%v+%v]%v%v", t.FileHashStoreKey, t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
}
