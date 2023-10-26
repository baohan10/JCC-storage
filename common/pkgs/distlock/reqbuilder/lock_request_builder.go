package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
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

func (b *LockRequestBuilder) MutexLock(svc *distlock.Service) (*distlock.Mutex, error) {
	mutex := distlock.NewMutex(svc, b.Build())
	err := mutex.Lock()
	if err != nil {
		return nil, err
	}

	return mutex, nil
}
