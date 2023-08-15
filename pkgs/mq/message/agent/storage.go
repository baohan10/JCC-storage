package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

// 客户端发给代理端，告知要调度多副本冗余的数据，以及要调度数据的详情
type StartStorageMoveObject struct {
	UserID     int64                 `json:"userID"`
	ObjectID   int64                 `json:"objectID"`
	ObjectName string                `json:"objectName"`
	Directory  string                `json:"directory"`
	FileSize   int64                 `json:"fileSize,string"`
	Redundancy models.RedundancyData `json:"redundancy"`
}

func NewStartStorageMoveObject[T models.RedundancyDataConst](dir string, objectID int64, objectName string, userID int64, fileSize int64, redundancy T) StartStorageMoveObject {
	return StartStorageMoveObject{
		Directory:  dir,
		ObjectID:   objectID,
		ObjectName: objectName,
		UserID:     userID,
		FileSize:   fileSize,
		Redundancy: redundancy,
	}
}

// 代理端发给客户端，告知调度的结果
type StartStorageMoveObjectResp struct {
	TaskID string `json:"taskID"`
}

func NewStartStorageMoveObjectResp(taskID string) StartStorageMoveObjectResp {
	return StartStorageMoveObjectResp{
		TaskID: taskID,
	}
}

type WaitStorageMoveObject struct {
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}

func NewWaitStorageMoveObject(taskID string, waitTimeoutMs int64) WaitStorageMoveObject {
	return WaitStorageMoveObject{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}

type WaitStorageMoveObjectResp struct {
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
}

func NewWaitStorageMoveObjectResp(isComplete bool, err string) WaitStorageMoveObjectResp {
	return WaitStorageMoveObjectResp{
		IsComplete: isComplete,
		Error:      err,
	}
}

type StorageCheck struct {
	StorageID  int64                 `json:"storageID"`
	Directory  string                `json:"directory"`
	IsComplete bool                  `json:"isComplete"`
	Objects    []model.StorageObject `json:"objects"`
}

func NewStorageCheck(storageID int64, directory string, isComplete bool, objects []model.StorageObject) StorageCheck {
	return StorageCheck{
		StorageID:  storageID,
		Directory:  directory,
		IsComplete: isComplete,
		Objects:    objects,
	}
}

type StorageCheckResp struct {
	DirectoryState string                  `json:"directoryState"`
	Entries        []StorageCheckRespEntry `json:"entries"`
}

const (
	CHECK_STORAGE_RESP_OP_DELETE     = "Delete"
	CHECK_STORAGE_RESP_OP_SET_NORMAL = "SetNormal"
)

type StorageCheckRespEntry struct {
	ObjectID  int64  `json:"objectID"`
	UserID    int64  `json:"userID"`
	Operation string `json:"operation"`
}

func NewStorageCheckRespEntry(objectID int64, userID int64, op string) StorageCheckRespEntry {
	return StorageCheckRespEntry{
		ObjectID:  objectID,
		UserID:    userID,
		Operation: op,
	}
}
func NewStorageCheckResp(dirState string, entries []StorageCheckRespEntry) StorageCheckResp {
	return StorageCheckResp{
		DirectoryState: dirState,
		Entries:        entries,
	}
}

type StartStorageUploadRepObject struct {
	UserID           int64  `json:"userID"`
	FilePath         string `json:"filePath"`
	BucketID         int64  `json:"bucketID"`
	ObjectName       string `json:"objectName"`
	RepCount         int    `json:"repCount"`
	StorageDirectory string `json:"storageDirectory"`
}

func NewStartStorageUploadRepObject(userID int64, filePath string, bucketID int64, objectName string, repCount int, storageDirectory string) StartStorageUploadRepObject {
	return StartStorageUploadRepObject{
		UserID:           userID,
		FilePath:         filePath,
		BucketID:         bucketID,
		ObjectName:       objectName,
		RepCount:         repCount,
		StorageDirectory: storageDirectory,
	}
}

type StartStorageUploadRepObjectResp struct {
	TaskID string `json:"taskID"`
}

func NewStartStorageUploadRepObjectResp(taskID string) StartStorageUploadRepObjectResp {
	return StartStorageUploadRepObjectResp{
		TaskID: taskID,
	}
}

type WaitStorageUploadRepObject struct {
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}

func NewWaitStorageUploadRepObject(taskID string, waitTimeoutMs int64) WaitStorageUploadRepObject {
	return WaitStorageUploadRepObject{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}

type WaitStorageUploadRepObjectResp struct {
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
	ObjectID   int64  `json:"objectID"`
	FileHash   string `json:"fileHash"`
}

func NewWaitStorageUploadRepObjectResp(isComplete bool, err string, objectID int64, fileHash string) WaitStorageUploadRepObjectResp {
	return WaitStorageUploadRepObjectResp{
		IsComplete: isComplete,
		Error:      err,
		ObjectID:   objectID,
		FileHash:   fileHash,
	}
}

func init() {
	mq.RegisterMessage[StartStorageMoveObject]()
	mq.RegisterMessage[StartStorageMoveObjectResp]()

	mq.RegisterMessage[WaitStorageMoveObject]()
	mq.RegisterMessage[WaitStorageMoveObjectResp]()

	mq.RegisterMessage[StorageCheck]()
	mq.RegisterMessage[StorageCheckResp]()

	mq.RegisterMessage[StartStorageUploadRepObject]()
	mq.RegisterMessage[StartStorageUploadRepObjectResp]()

	mq.RegisterMessage[WaitStorageUploadRepObject]()
	mq.RegisterMessage[WaitStorageUploadRepObjectResp]()
}
