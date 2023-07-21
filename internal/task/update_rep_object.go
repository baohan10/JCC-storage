package task

import (
	"fmt"
	"io"
	"time"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"

	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

type UpdateRepObject struct {
	userID   int64
	objectID int64
	file     io.ReadCloser
	fileSize int64
}

func NewUpdateRepObject(userID int64, objectID int64, file io.ReadCloser, fileSize int64) *UpdateRepObject {
	return &UpdateRepObject{
		userID:   userID,
		objectID: objectID,
		file:     file,
		fileSize: fileSize,
	}
}

func (t *UpdateRepObject) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *UpdateRepObject) do(ctx TaskContext) error {
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有对象的权限
		UserBucket().ReadAny().
		// 用于读取、修改对象信息
		Object().WriteOne(t.objectID).
		// 用于更新Rep配置
		ObjectRep().WriteOne(t.objectID).
		// 用于查询可用的上传节点
		Node().ReadAny().
		// 用于创建Cache记录
		Cache().CreateAny().
		// 用于修改Move此Object的记录的状态
		StorageObject().WriteAny().
		MutexLock(ctx.DistLock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	preResp, err := ctx.Coordinator.PreUpdateRepObject(coormsg.NewPreUpdateRepObject(
		t.objectID,
		t.fileSize,
		t.userID,
		config.Cfg().ExternalIP,
	))
	if err != nil {
		return fmt.Errorf("pre update rep object: %w", err)
	}

	if len(preResp.Nodes) == 0 {
		return fmt.Errorf("no node to upload file")
	}

	// 上传文件的方式优先级：
	// 1. 本地IPFS
	// 2. 包含了旧文件，且与客户端在同地域的节点
	// 3. 不在同地域，但包含了旧文件的节点
	// 4. 同地域节点

	uploadNode := t.chooseUpdateRepObjectNode(preResp.Nodes)

	var fileHash string
	uploadedNodeIDs := []int64{}
	willUploadToNode := true
	// 本地有IPFS，则直接从本地IPFS上传
	if ctx.IPFS != nil {
		logger.Infof("try to use local IPFS to upload file")

		fileHash, err = uploadToLocalIPFS(ctx.IPFS, t.file, uploadNode.ID)
		if err != nil {
			logger.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.ID, err.Error())
		} else {
			willUploadToNode = false
		}
	}

	// 否则发送到agent上传
	if willUploadToNode {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := uploadNode.ExternalIP
		if uploadNode.IsSameLocation {
			nodeIP = uploadNode.LocalIP

			logger.Infof("client and node %d are at the same location, use local ip\n", uploadNode.ID)
		}

		mutex, err := reqbuilder.NewBuilder().
			IPFS().
			// 防止上传的副本被清除
			CreateAnyRep(uploadNode.ID).
			MutexLock(ctx.DistLock)
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()

		fileHash, err = uploadToNode(t.file, nodeIP)
		if err != nil {
			return fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
		}
		uploadedNodeIDs = append(uploadedNodeIDs, uploadNode.ID)
	}

	// 更新Object
	_, err = ctx.Coordinator.UpdateRepObject(coormsg.NewUpdateRepObject(t.objectID, fileHash, t.fileSize, uploadedNodeIDs, t.userID))
	if err != nil {
		return fmt.Errorf("updating rep object: %w", err)
	}

	return nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *UpdateRepObject) chooseUpdateRepObjectNode(nodes []coormsg.PreUpdateRepObjectRespNode) coormsg.PreUpdateRepObjectRespNode {
	mysort.Sort(nodes, func(left, right coormsg.PreUpdateRepObjectRespNode) int {
		v := -mysort.CmpBool(left.HasOldObject, right.HasOldObject)
		if v != 0 {
			return v
		}

		return -mysort.CmpBool(left.IsSameLocation, right.IsSameLocation)
	})

	return nodes[0]
}
