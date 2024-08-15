package ops

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
)

func init() {
	OpUnion.AddT((*IPFSRead)(nil))
	OpUnion.AddT((*IPFSWrite)(nil))
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

func (t *IPFSReadType) InitNode(node *Node) {
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *IPFSReadType) GenerateOp(node *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&IPFSRead{
		Output:   node.OutputStreams[0].Props.Var.(*exec.StreamVar),
		FileHash: t.FileHash,
		Option:   t.Option,
	}, node.Env, blder)
	return nil
}

func (t *IPFSReadType) String(node *Node) string {
	return fmt.Sprintf("IPFSRead[%s,%v+%v]%v%v", t.FileHash, t.Option.Offset, t.Option.Length, formatStreamIO(node), formatValueIO(node))
}

type IPFSWriteType struct {
	FileHashStoreKey string
	Range            exec.Range
}

func (t *IPFSWriteType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputValue(node, VarProps{})
}

func (t *IPFSWriteType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&IPFSWrite{
		Input:    op.InputStreams[0].Props.Var.(*exec.StreamVar),
		FileHash: op.OutputValues[0].Props.Var.(*exec.StringVar),
	}, op.Env, blder)
	return nil
}

func (t *IPFSWriteType) String(node *Node) string {
	return fmt.Sprintf("IPFSWrite[%s,%v+%v](%v>)", t.FileHashStoreKey, t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
}
