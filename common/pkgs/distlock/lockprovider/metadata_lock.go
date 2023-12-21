package lockprovider

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	mylo "gitlink.org.cn/cloudream/common/utils/lo"
)

const (
	MetadataLockPathPrefix = "Metadata"
	MetadataCreateLock     = "Create"
)

type metadataElementLock struct {
	target     StringLockTarget
	requestIDs []string
}

type MetadataLock struct {
	createReqIDs []*metadataElementLock

	lockCompatibilityTable LockCompatibilityTable
}

func NewMetadataLock() *MetadataLock {

	metadataLock := MetadataLock{
		lockCompatibilityTable: LockCompatibilityTable{},
	}

	compTable := &metadataLock.lockCompatibilityTable

	compTable.
		Column(MetadataCreateLock, func() bool { return len(metadataLock.createReqIDs) > 0 })
	trgt := LockSpecial(func(lock distlock.Lock, testLockName string) bool {
		strTar := lock.Target.(StringLockTarget)
		return lo.NoneBy(metadataLock.createReqIDs, func(other *metadataElementLock) bool { return strTar.IsConflict(&other.target) })
	})

	compTable.MustRow(trgt)

	return &metadataLock
}

// CanLock 判断这个锁能否锁定成功
func (l *MetadataLock) CanLock(lock distlock.Lock) error {
	return l.lockCompatibilityTable.Test(lock)
}

// 锁定
func (l *MetadataLock) Lock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case MetadataCreateLock:
		l.createReqIDs = l.addElementLock(lock, l.createReqIDs, reqID)

	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

func (l *MetadataLock) addElementLock(lock distlock.Lock, locks []*metadataElementLock, reqID string) []*metadataElementLock {
	strTarget := lock.Target.(StringLockTarget)
	lck, ok := lo.Find(locks, func(l *metadataElementLock) bool { return strTarget.IsConflict(&l.target) })
	if !ok {
		lck = &metadataElementLock{
			target: strTarget,
		}
		locks = append(locks, lck)
	}

	lck.requestIDs = append(lck.requestIDs, reqID)
	return locks
}

// 解锁
func (l *MetadataLock) Unlock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case MetadataCreateLock:
		l.createReqIDs = l.removeElementLock(lock, l.createReqIDs, reqID)

	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

func (l *MetadataLock) removeElementLock(lock distlock.Lock, locks []*metadataElementLock, reqID string) []*metadataElementLock {
	strTarget := lock.Target.(StringLockTarget)
	lck, index, ok := lo.FindIndexOf(locks, func(l *metadataElementLock) bool { return strTarget.IsConflict(&l.target) })
	if !ok {
		return locks
	}

	lck.requestIDs = mylo.Remove(lck.requestIDs, reqID)

	if len(lck.requestIDs) == 0 {
		locks = mylo.RemoveAt(locks, index)
	}

	return locks
}

// GetTargetString 将锁对象序列化为字符串，方便存储到ETCD
func (l *MetadataLock) GetTargetString(target any) (string, error) {
	tar := target.(StringLockTarget)
	return StringLockTargetToString(&tar)
}

// ParseTargetString 解析字符串格式的锁对象数据
func (l *MetadataLock) ParseTargetString(targetStr string) (any, error) {
	return StringLockTargetFromString(targetStr)
}

// Clear 清除内部所有状态
func (l *MetadataLock) Clear() {
	l.createReqIDs = nil
}
