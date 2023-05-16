package cmdline

import (
	"fmt"
	"os"
	"strconv"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/client/internal/services"
)

type Commandline struct {
	svc *services.Service
}

func NewCommandline(svc *services.Service) (*Commandline, error) {
	return &Commandline{
		svc: svc,
	}, nil
}

func (c *Commandline) DispatchCommand(cmd string, args []string) {
	switch cmd {
	case "read":
		objectID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}

		if err := c.Read(args[0], objectID); err != nil {
			fmt.Printf("read failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "write":
		bucketID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}
		repCount, _ := strconv.Atoi(args[3])
		if repCount <= 0 || repCount > config.Cfg().MaxRepCount {
			fmt.Printf("replicate number should not be more than %d", config.Cfg().MaxRepCount)
			os.Exit(1)
		}

		if err := c.RepWrite(args[0], bucketID, args[2], repCount); err != nil {
			fmt.Printf("rep write failed, err: %s", err.Error())
			os.Exit(1)
		}
	case "ecWrite":
		bucketID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}
		if err := c.EcWrite(args[0], bucketID, args[2], args[3]); err != nil {
			fmt.Printf("ec write failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "storage":
		if len(args) == 0 {
			fmt.Printf("need more arg")
			os.Exit(1)
		}
		cmd := args[0]

		switch cmd {
		case "move":
			objectID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}
			stgID, err := strconv.Atoi(args[2])
			if err != nil {
				fmt.Printf("invalid storage id %s, err: %s", args[2], err.Error())
				os.Exit(1)
			}

			if err := c.MoveObjectToStorage(objectID, stgID); err != nil {
				fmt.Printf("move failed, err: %s", err.Error())
				os.Exit(1)
			}
		}

	case "bucket":
		if len(args) == 0 {
			fmt.Printf("need more arg")
			os.Exit(1)
		}
		cmd := args[0]

		switch cmd {
		case "ls":
			if err := c.ListUserBuckets(); err != nil {
				fmt.Printf("get user buckets failed, err: %s", err.Error())
				os.Exit(1)
			}

		case "new":
			if err := c.CreateBucket(args[1]); err != nil {
				fmt.Printf("create bucket failed, err: %s", err.Error())
				os.Exit(1)
			}

		case "delete":
			bucketID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.DeleteBucket(bucketID); err != nil {
				fmt.Printf("delete bucket failed, err: %s", err.Error())
				os.Exit(1)
			}
		}

	case "object":
		if len(args) == 0 {
			fmt.Printf("need more arg")
			os.Exit(1)
		}

		switch args[0] {
		case "ls":
			bucketID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.ListBucketObjects(bucketID); err != nil {
				fmt.Printf("get bucket objects failed, err: %s", err.Error())
				os.Exit(1)
			}

		case "update":
			objectID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.UpdateRepObject(objectID, args[2]); err != nil {
				fmt.Printf("delete object failed, err: %s", err.Error())
				os.Exit(1)
			}

		case "delete":
			objectID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.DeleteObject(objectID); err != nil {
				fmt.Printf("delete object failed, err: %s", err.Error())
				os.Exit(1)
			}
		}
	}

}
