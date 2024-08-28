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
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	lrcparser "gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/parser"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func init() {
	rootCmd.AddCommand(&cobra.Command{
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

			ft.AddFrom(ioswitch2.NewFromNode("Qmf412jQZiuVUtdgnB36FXFX7xg5V6KEbSJ4dpQuhkLyfD", &nodes.Nodes[0], -1))
			ft.AddTo(ioswitch2.NewToNode(nodes.Nodes[1], -1, "asd"))
			le := int64(3)
			toExec, hd := ioswitch2.NewToDriverWithRange(-1, exec.Range{Offset: 5, Length: &le})
			ft.AddTo(toExec)
			ft.AddTo(ioswitch2.NewToNode(nodes.Nodes[1], 0, "0"))
			ft.AddTo(ioswitch2.NewToNode(nodes.Nodes[1], 1, "1"))
			ft.AddTo(ioswitch2.NewToNode(nodes.Nodes[1], 2, "2"))

			// ft.AddFrom(ioswitch2.NewFromNode("QmS2s8GRYHEurXL7V1zUtKvf2H1BGcQc5NN1T1hiSnWvbd", &nodes.Nodes[0], 1))
			// ft.AddFrom(ioswitch2.NewFromNode("QmUgUEUMzdnjPNx6xu9PDGXpSyXTk8wzPWvyYZ9zasE1WW", &nodes.Nodes[1], 2))
			// le := int64(5)
			// toExec, hd := ioswitch2.NewToDriverWithRange(-1, exec.Range{Offset: 3, Length: &le})
			// toExec, hd := plans.NewToExecutorWithRange(1, plans.Range{Offset: 0, Length: nil})
			// toExec2, hd2 := plans.NewToExecutorWithRange(2, plans.Range{Offset: 0, Length: nil})
			// ft.AddTo(toExec)
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
	})

	// rootCmd.AddCommand(&cobra.Command{
	// 	Use:   "test",
	// 	Short: "test",
	// 	// Args:  cobra.ExactArgs(1),
	// 	Run: func(cmd *cobra.Command, args []string) {
	// 		cmdCtx := GetCmdCtx(cmd)
	// 		file, _ := cmdCtx.Cmdline.Svc.ObjectSvc().Download(1, downloader.DownloadReqeust{
	// 			ObjectID: 27379,
	// 			Length:   -1,
	// 		})
	// 		data, _ := io.ReadAll(file.File)
	// 		fmt.Printf("data: %v(%v)\n", string(data), len(data))
	// 	},
	// })

	rootCmd.AddCommand(&cobra.Command{
		Use:   "test3",
		Short: "test3",
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

			red := cdssdk.DefaultLRCRedundancy

			var toes []ioswitchlrc.To
			for i := 0; i < red.N; i++ {
				toes = append(toes, ioswitchlrc.NewToNode(nodes.Nodes[i%2], i, fmt.Sprintf("%d", i)))
			}

			plans := exec.NewPlanBuilder()
			err = lrcparser.Encode(ioswitchlrc.NewFromNode("QmNspjDLxQbAsuh37jRXKvLWHE2f7JpqY4HEJ8x7Jgbzqa", &nodes.Nodes[0], -1), toes, plans)
			if err != nil {
				panic(err)
				// return nil, fmt.Errorf("parsing plan: %w", err)
			}

			ioRet, err := plans.Execute().Wait(context.TODO())
			if err != nil {
				panic(err)
				// return nil, fmt.Errorf("executing io plan: %w", err)
			}

			fmt.Printf("ioRet: %v\n", ioRet)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "test",
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

			// red := cdssdk.DefaultLRCRedundancy

			plans := exec.NewPlanBuilder()
			err = lrcparser.ReconstructGroup([]ioswitchlrc.From{
				ioswitchlrc.NewFromNode("QmVAZzVQEvnvTvzSz2SvpziAcDSQ8aYCoTyGrZNuV8raEQ", &nodes.Nodes[1], 0),
				ioswitchlrc.NewFromNode("QmVAZzVQEvnvTvzSz2SvpziAcDSQ8aYCoTyGrZNuV8raEQ", &nodes.Nodes[1], 1),
			}, []ioswitchlrc.To{
				ioswitchlrc.NewToNode(nodes.Nodes[1], 3, "3"),
			}, plans)
			if err != nil {
				panic(err)
				// return nil, fmt.Errorf("parsing plan: %w", err)
			}

			ioRet, err := plans.Execute().Wait(context.TODO())
			if err != nil {
				panic(err)
				// return nil, fmt.Errorf("executing io plan: %w", err)
			}

			fmt.Printf("ioRet: %v\n", ioRet)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "test4",
		Short: "test4",
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

			// red := cdssdk.DefaultLRCRedundancy

			plans := exec.NewPlanBuilder()
			le := int64(1293)
			err = lrcparser.ReconstructAny([]ioswitchlrc.From{
				ioswitchlrc.NewFromNode("QmVAZzVQEvnvTvzSz2SvpziAcDSQ8aYCoTyGrZNuV8raEQ", &nodes.Nodes[0], 0),
				ioswitchlrc.NewFromNode("QmQBKncEDqxw3BrGr3th3gS3jUC2fizGz1w29ZxxrrKfNv", &nodes.Nodes[0], 2),
			}, []ioswitchlrc.To{
				ioswitchlrc.NewToNodeWithRange(nodes.Nodes[1], -1, "-1", exec.Range{0, &le}),
				ioswitchlrc.NewToNode(nodes.Nodes[1], 0, "0"),
				ioswitchlrc.NewToNode(nodes.Nodes[1], 1, "1"),
				ioswitchlrc.NewToNode(nodes.Nodes[1], 2, "2"),
				ioswitchlrc.NewToNode(nodes.Nodes[1], 3, "3"),
			}, plans)
			if err != nil {
				panic(err)
				// return nil, fmt.Errorf("parsing plan: %w", err)
			}

			ioRet, err := plans.Execute().Wait(context.TODO())
			if err != nil {
				panic(err)
				// return nil, fmt.Errorf("executing io plan: %w", err)
			}

			fmt.Printf("ioRet: %v\n", ioRet)
		},
	})
}
