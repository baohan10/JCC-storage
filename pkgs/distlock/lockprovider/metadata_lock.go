package lockprovider

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	mylo "gitlink.org.cn/cloudream/common/utils/lo"
)

const (
	MetadataLockPathPrefix = "Metadata"

	METADATA_SET_READ_LOCK   = "SetRead"
	METADATA_SET_WRITE_LOCK  = "SetWrite"
	METADATA_SET_CREATE_LOCK = "SetCreate"

	METADATA_ELEMENT_READ_LOCK   = "ElementRead"
	METADATA_ELEMENT_WRITE_LOCK  = "ElementWrite"
	METADATA_ELEMENT_CREATE_LOCK = "ElementCreate"
)

type metadataElementLock struct {
	target     StringLockTarget
	requestIDs []string
}

type MetadataLock struct {
	setReadReqIDs   []string
	setWriteReqIDs  []string
	setCreateReqIDs []string

	elementReadLocks   []*metadataElementLock
	elementWriteLocks  []*metadataElementLock
	elementCreateLocks []*metadataElementLock

	lockCompatibilityTable LockCompatibilityTable
}

func NewMetadataLock() *MetadataLock {

	metadataLock := MetadataLock{
		lockCompatibilityTable: LockCompatibilityTable{},
	}

	compTable := &metadataLock.lockCompatibilityTable

	compTable.
		Column(METADATA_ELEMENT_READ_LOCK, func() bool { return len(metadataLock.elementReadLocks) > 0 }).
		Column(METADATA_ELEMENT_WRITE_LOCK, func() bool { return len(metadataLock.elementWriteLocks) > 0 }).
		Column(METADATA_ELEMENT_CREATE_LOCK, func() bool { return len(metadataLock.elementCreateLocks) > 0 }).
		Column(METADATA_SET_READ_LOCK, func() bool { return len(metadataLock.setReadReqIDs) > 0 }).
		Column(METADATA_SET_WRITE_LOCK, func() bool { return len(metadataLock.setWriteReqIDs) > 0 }).
		Column(METADATA_SET_CREATE_LOCK, func() bool { return len(metadataLock.setCreateReqIDs) > 0 })

	comp := LockCompatible()
	uncp := LockUncompatible()
	trgt := LockSpecial(func(lock distlock.Lock, testLockName string) bool {
		strTar := lock.Target.(StringLockTarget)
		if testLockName == METADATA_ELEMENT_READ_LOCK {
			// 如果没有任何锁的锁对象与当前的锁对象冲突，那么这个锁可以加
			return lo.NoneBy(metadataLock.elementReadLocks, func(other *metadataElementLock) bool { return strTar.IsConflict(&other.target) })
		}

		if testLockName == METADATA_ELEMENT_WRITE_LOCK {
			return lo.NoneBy(metadataLock.elementWriteLocks, func(other *metadataElementLock) bool { return strTar.IsConflict(&other.target) })
		}

		return lo.NoneBy(metadataLock.elementCreateLocks, func(other *metadataElementLock) bool { return strTar.IsConflict(&other.target) })
	})

	compTable.MustRow(comp, trgt, comp, comp, uncp, comp)
	compTable.MustRow(trgt, trgt, comp, uncp, uncp, comp)
	compTable.MustRow(comp, comp, trgt, uncp, uncp, uncp)
	compTable.MustRow(comp, uncp, uncp, comp, uncp, uncp)
	compTable.MustRow(uncp, uncp, uncp, uncp, uncp, uncp)
	compTable.MustRow(comp, comp, uncp, uncp, uncp, uncp)

	return &metadataLock
}

// CanLock 判断这个锁能否锁定成功
func (l *MetadataLock) CanLock(lock distlock.Lock) error {
	return l.lockCompatibilityTable.Test(lock)
}

// 锁定
func (l *MetadataLock) Lock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case METADATA_SET_READ_LOCK:
		l.setReadReqIDs = append(l.setReadReqIDs, reqID)
	case METADATA_SET_WRITE_LOCK:
		l.setWriteReqIDs = append(l.setWriteReqIDs, reqID)
	case METADATA_SET_CREATE_LOCK:
		l.setCreateReqIDs = append(l.setCreateReqIDs, reqID)

	case METADATA_ELEMENT_READ_LOCK:
		l.elementReadLocks = l.addElementLock(lock, l.elementReadLocks, reqID)
	case METADATA_ELEMENT_WRITE_LOCK:
		l.elementWriteLocks = l.addElementLock(lock, l.elementWriteLocks, reqID)
	case METADATA_ELEMENT_CREATE_LOCK:
		l.elementCreateLocks = l.addElementLock(lock, l.elementCreateLocks, reqID)

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
	case METADATA_SET_READ_LOCK:
		l.setReadReqIDs = mylo.Remove(l.setReadReqIDs, reqID)
	case METADATA_SET_WRITE_LOCK:
		l.setWriteReqIDs = mylo.Remove(l.setWriteReqIDs, reqID)
	case METADATA_SET_CREATE_LOCK:
		l.setCreateReqIDs = mylo.Remove(l.setCreateReqIDs, reqID)

	case METADATA_ELEMENT_READ_LOCK:
		l.elementReadLocks = l.removeElementLock(lock, l.elementReadLocks, reqID)
	case METADATA_ELEMENT_WRITE_LOCK:
		l.elementWriteLocks = l.removeElementLock(lock, l.elementWriteLocks, reqID)
	case METADATA_ELEMENT_CREATE_LOCK:
		l.elementCreateLocks = l.removeElementLock(lock, l.elementCreateLocks, reqID)

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
	l.setReadReqIDs = nil
	l.setWriteReqIDs = nil
	l.setCreateReqIDs = nil
	l.elementReadLocks = nil
	l.elementWriteLocks = nil
	l.elementCreateLocks = nil
}
