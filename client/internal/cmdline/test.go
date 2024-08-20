package cmdline

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func init() {
	cmd := &cobra.Command{
		Use:   "test2",
		Short: "test2",
		// Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// cmdCtx := GetCmdCtx(cmd)
			coorCli, err := stgglb.CoordinatorMQPool.Acquire()
			if err != nil {
				panic(err)
			}
			defer stgglb.CoordinatorMQPool.Release(coorCli)

			nodes, err := coorCli.GetNodes(coormq.NewGetNodes([]cdssdk.NodeID{1, 2}))
			if err != nil {
				panic(err)
			}

			ft := ioswitch2.NewFromTo()

			// ft.AddFrom(plans.NewFromNode("Qmf412jQZiuVUtdgnB36FXFX7xg5V6KEbSJ4dpQuhkLyfD", &nodes.Nodes[0], -1))
			// ft.AddTo(plans.NewToNode(nodes.Nodes[1], -1, "asd"))
			// len := int64(3)
			// toExec, hd := plans.NewToExecutorWithRange(-1, plans.Range{Offset: 5, Length: &len})
			// ft.AddTo(toExec)
			// ft.AddTo(plans.NewToNode(nodes.Nodes[1], 0, "0"))
			// ft.AddTo(plans.NewToNode(nodes.Nodes[1], 1, "1"))
			// ft.AddTo(plans.NewToNode(nodes.Nodes[1], 2, "2"))

			ft.AddFrom(ioswitch2.NewFromNode("QmS2s8GRYHEurXL7V1zUtKvf2H1BGcQc5NN1T1hiSnWvbd", &nodes.Nodes[0], 1))
			ft.AddFrom(ioswitch2.NewFromNode("QmUgUEUMzdnjPNx6xu9PDGXpSyXTk8wzPWvyYZ9zasE1WW", &nodes.Nodes[1], 2))
			le := int64(3)
			toExec, hd := ioswitch2.NewToDriverWithRange(-1, exec.Range{Offset: 5, Length: &le})
			// toExec, hd := plans.NewToExecutorWithRange(1, plans.Range{Offset: 0, Length: nil})
			// toExec2, hd2 := plans.NewToExecutorWithRange(2, plans.Range{Offset: 0, Length: nil})
			ft.AddTo(toExec)
			// ft.AddTo(toExec2)

			// fromExec, hd := plans.NewFromExecutor(-1)
			// ft.AddFrom(fromExec)
			// ft.AddTo(plans.NewToNode(nodes.Nodes[1], -1, "asd"))

			parser := parser.NewParser(cdssdk.DefaultECRedundancy)

			plans := exec.NewPlanBuilder()
			err = parser.Parse(ft, plans)
			if err != nil {
				panic(err)
			}

			exec := plans.Execute()

			fut := future.NewSetVoid()
			go func() {
				mp, err := exec.Wait(context.Background())
				if err != nil {
					panic(err)
				}

				fmt.Printf("mp: %+v\n", mp)
				fut.SetVoid()
			}()

			go func() {
				// exec.BeginWrite(io.NopCloser(bytes.NewBuffer([]byte("hello world"))), hd)
				// if err != nil {
				// 	panic(err)
				// }

				str, err := exec.BeginRead(hd)
				if err != nil {
					panic(err)
				}
				defer str.Close()
				data, err := io.ReadAll(str)
				if err != nil && err != io.EOF {
					panic(err)
				}
				fmt.Printf("data: %v(%v)\n", string(data), len(data))
			}()

			fut.Wait(context.TODO())
		},
	}

	cmd2 := &cobra.Command{
		Use:   "test",
		Short: "test",
		// Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx := GetCmdCtx(cmd)
			file, _ := cmdCtx.Cmdline.Svc.ObjectSvc().Download(1, downloader.DownloadReqeust{
				ObjectID: 27379,
				Length:   -1,
			})
			data, _ := io.ReadAll(file.File)
			fmt.Printf("data: %v(%v)\n", string(data), len(data))
		},
	}

	rootCmd.AddCommand(cmd)
	rootCmd.AddCommand(cmd2)
}
