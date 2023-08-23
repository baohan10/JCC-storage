package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	mylo "gitlink.org.cn/cloudream/common/utils/lo"
)

type LockRequestBuilder struct {
	locks []distlock.Lock
}

func NewBuilder() *LockRequestBuilder {
	return &LockRequestBuilder{}
}

func (b *LockRequestBuilder) Build() distlock.LockRequest {
	return distlock.LockRequest{
		Locks: mylo.ArrayClone(b.locks),
	}
}

func (b *LockRequestBuilder) MutexLock(svc *service.Service) (*service.Mutex, error) {
	mutex := service.NewMutex(svc, b.Build())
	err := mutex.Lock()
	if err != nil {
		return nil, err
	}

	return mutex, nil
}
