package lockprovider

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	mylo "gitlink.org.cn/cloudream/common/utils/lo"
)

const (
	StorageLockPathPrefix = "Storage"

	STORAGE_SET_READ_LOCK   = "SetRead"
	STORAGE_SET_WRITE_LOCK  = "SetWrite"
	STORAGE_SET_CREATE_LOCK = "SetCreate"

	STORAGE_ELEMENT_READ_LOCK   = "ElementRead"
	STORAGE_ELEMENT_WRITE_LOCK  = "ElementWrite"
	STORAGE_ELEMENT_CREATE_LOCK = "ElementCreate"

	STORAGE_STORAGE_ID_PATH_INDEX = 1
)

type StorageLock struct {
	nodeLocks map[string]*StorageNodeLock
	dummyLock *StorageNodeLock
}

func NewStorageLock() *StorageLock {
	return &StorageLock{
		nodeLocks: make(map[string]*StorageNodeLock),
		dummyLock: NewStorageNodeLock(),
	}
}

// CanLock 判断这个锁能否锁定成功
func (l *StorageLock) CanLock(lock distlock.Lock) error {
	nodeLock, ok := l.nodeLocks[lock.Path[STORAGE_STORAGE_ID_PATH_INDEX]]
	if !ok {
		// 不能直接返回nil，因为如果锁数据的格式不对，也不能获取锁。
		// 这里使用一个空Provider来进行检查。
		return l.dummyLock.CanLock(lock)
	}

	return nodeLock.CanLock(lock)
}

// 锁定。在内部可以不用判断能否加锁，外部需要保证调用此函数前调用了CanLock进行检查
func (l *StorageLock) Lock(reqID string, lock distlock.Lock) error {
	nodeID := lock.Path[STORAGE_STORAGE_ID_PATH_INDEX]

	nodeLock, ok := l.nodeLocks[nodeID]
	if !ok {
		nodeLock = NewStorageNodeLock()
		l.nodeLocks[nodeID] = nodeLock
	}

	return nodeLock.Lock(reqID, lock)
}

// 解锁
func (l *StorageLock) Unlock(reqID string, lock distlock.Lock) error {
	nodeID := lock.Path[STORAGE_STORAGE_ID_PATH_INDEX]

	nodeLock, ok := l.nodeLocks[nodeID]
	if !ok {
		return nil
	}

	return nodeLock.Unlock(reqID, lock)
}

// GetTargetString 将锁对象序列化为字符串，方便存储到ETCD
func (l *StorageLock) GetTargetString(target any) (string, error) {
	tar := target.(StringLockTarget)
	return StringLockTargetToString(&tar)
}

// ParseTargetString 解析字符串格式的锁对象数据
func (l *StorageLock) ParseTargetString(targetStr string) (any, error) {
	return StringLockTargetFromString(targetStr)
}

// Clear 清除内部所有状态
func (l *StorageLock) Clear() {
	l.nodeLocks = make(map[string]*StorageNodeLock)
}

type storageElementLock struct {
	target     StringLockTarget
	requestIDs []string
}

type StorageNodeLock struct {
	setReadReqIDs   []string
	setWriteReqIDs  []string
	setCreateReqIDs []string

	elementReadLocks   []*storageElementLock
	elementWriteLocks  []*storageElementLock
	elementCreateLocks []*storageElementLock

	lockCompatibilityTable LockCompatibilityTable
}

func NewStorageNodeLock() *StorageNodeLock {

	storageLock := StorageNodeLock{
		lockCompatibilityTable: LockCompatibilityTable{},
	}

	compTable := &storageLock.lockCompatibilityTable

	compTable.
		Column(STORAGE_ELEMENT_READ_LOCK, func() bool { return len(storageLock.elementReadLocks) > 0 }).
		Column(STORAGE_ELEMENT_WRITE_LOCK, func() bool { return len(storageLock.elementWriteLocks) > 0 }).
		Column(STORAGE_ELEMENT_CREATE_LOCK, func() bool { return len(storageLock.elementCreateLocks) > 0 }).
		Column(STORAGE_SET_READ_LOCK, func() bool { return len(storageLock.setReadReqIDs) > 0 }).
		Column(STORAGE_SET_WRITE_LOCK, func() bool { return len(storageLock.setWriteReqIDs) > 0 }).
		Column(STORAGE_SET_CREATE_LOCK, func() bool { return len(storageLock.setCreateReqIDs) > 0 })

	comp := LockCompatible()
	uncp := LockUncompatible()
	trgt := LockSpecial(func(lock distlock.Lock, testLockName string) bool {
		strTar := lock.Target.(StringLockTarget)
		if testLockName == STORAGE_ELEMENT_READ_LOCK {
			// 如果没有任何锁的锁对象与当前的锁对象冲突，那么这个锁可以加
			return lo.NoneBy(storageLock.elementReadLocks, func(other *storageElementLock) bool { return strTar.IsConflict(&other.target) })
		}

		if testLockName == STORAGE_ELEMENT_WRITE_LOCK {
			return lo.NoneBy(storageLock.elementWriteLocks, func(other *storageElementLock) bool { return strTar.IsConflict(&other.target) })
		}

		return lo.NoneBy(storageLock.elementCreateLocks, func(other *storageElementLock) bool { return strTar.IsConflict(&other.target) })
	})

	compTable.MustRow(comp, trgt, comp, comp, uncp, comp)
	compTable.MustRow(trgt, trgt, comp, uncp, uncp, comp)
	compTable.MustRow(comp, comp, trgt, uncp, uncp, uncp)
	compTable.MustRow(comp, uncp, uncp, comp, uncp, uncp)
	compTable.MustRow(uncp, uncp, uncp, uncp, uncp, uncp)
	compTable.MustRow(comp, comp, uncp, uncp, uncp, uncp)

	return &storageLock
}

// CanLock 判断这个锁能否锁定成功
func (l *StorageNodeLock) CanLock(lock distlock.Lock) error {
	return l.lockCompatibilityTable.Test(lock)
}

// 锁定
func (l *StorageNodeLock) Lock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case STORAGE_SET_READ_LOCK:
		l.setReadReqIDs = append(l.setReadReqIDs, reqID)
	case STORAGE_SET_WRITE_LOCK:
		l.setWriteReqIDs = append(l.setWriteReqIDs, reqID)
	case STORAGE_SET_CREATE_LOCK:
		l.setCreateReqIDs = append(l.setCreateReqIDs, reqID)

	case STORAGE_ELEMENT_READ_LOCK:
		l.elementReadLocks = l.addElementLock(lock, l.elementReadLocks, reqID)
	case STORAGE_ELEMENT_WRITE_LOCK:
		l.elementWriteLocks = l.addElementLock(lock, l.elementWriteLocks, reqID)

	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

func (l *StorageNodeLock) addElementLock(lock distlock.Lock, locks []*storageElementLock, reqID string) []*storageElementLock {
	strTarget := lock.Target.(StringLockTarget)
	lck, ok := lo.Find(locks, func(l *storageElementLock) bool { return strTarget.IsConflict(&l.target) })
	if !ok {
		lck = &storageElementLock{
			target: strTarget,
		}
		locks = append(locks, lck)
	}

	lck.requestIDs = append(lck.requestIDs, reqID)
	return locks
}

// 解锁
func (l *StorageNodeLock) Unlock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case STORAGE_SET_READ_LOCK:
		l.setReadReqIDs = mylo.Remove(l.setReadReqIDs, reqID)
	case STORAGE_SET_WRITE_LOCK:
		l.setWriteReqIDs = mylo.Remove(l.setWriteReqIDs, reqID)
	case STORAGE_SET_CREATE_LOCK:
		l.setCreateReqIDs = mylo.Remove(l.setCreateReqIDs, reqID)

	case STORAGE_ELEMENT_READ_LOCK:
		l.elementReadLocks = l.removeElementLock(lock, l.elementReadLocks, reqID)
	case STORAGE_ELEMENT_WRITE_LOCK:
		l.elementWriteLocks = l.removeElementLock(lock, l.elementWriteLocks, reqID)

	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

func (l *StorageNodeLock) removeElementLock(lock distlock.Lock, locks []*storageElementLock, reqID string) []*storageElementLock {
	strTarget := lock.Target.(StringLockTarget)
	lck, index, ok := lo.FindIndexOf(locks, func(l *storageElementLock) bool { return strTarget.IsConflict(&l.target) })
	if !ok {
		return locks
	}

	lck.requestIDs = mylo.Remove(lck.requestIDs, reqID)

	if len(lck.requestIDs) == 0 {
		locks = mylo.RemoveAt(locks, index)
	}

	return locks
}
