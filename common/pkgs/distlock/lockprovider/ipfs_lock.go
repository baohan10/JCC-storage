package lockprovider

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/utils/lo2"
)

const (
	IPFSLockPathPrefix  = "IPFS"
	IPFSNodeIDPathIndex = 1
	IPFSBuzyLock        = "Buzy"
	IPFSGCLock          = "GC"
)

type IPFSLock struct {
	nodeLocks map[string]*IPFSNodeLock
	dummyLock *IPFSNodeLock
}

func NewIPFSLock() *IPFSLock {
	return &IPFSLock{
		nodeLocks: make(map[string]*IPFSNodeLock),
		dummyLock: NewIPFSNodeLock(),
	}
}

// CanLock 判断这个锁能否锁定成功
func (l *IPFSLock) CanLock(lock distlock.Lock) error {
	nodeLock, ok := l.nodeLocks[lock.Path[IPFSNodeIDPathIndex]]
	if !ok {
		// 不能直接返回nil，因为如果锁数据的格式不对，也不能获取锁。
		// 这里使用一个空Provider来进行检查。
		return l.dummyLock.CanLock(lock)
	}

	return nodeLock.CanLock(lock)
}

// 锁定。在内部可以不用判断能否加锁，外部需要保证调用此函数前调用了CanLock进行检查
func (l *IPFSLock) Lock(reqID string, lock distlock.Lock) error {
	nodeID := lock.Path[IPFSNodeIDPathIndex]

	nodeLock, ok := l.nodeLocks[nodeID]
	if !ok {
		nodeLock = NewIPFSNodeLock()
		l.nodeLocks[nodeID] = nodeLock
	}

	return nodeLock.Lock(reqID, lock)
}

// 解锁
func (l *IPFSLock) Unlock(reqID string, lock distlock.Lock) error {
	nodeID := lock.Path[IPFSNodeIDPathIndex]

	nodeLock, ok := l.nodeLocks[nodeID]
	if !ok {
		return nil
	}

	return nodeLock.Unlock(reqID, lock)
}

// GetTargetString 将锁对象序列化为字符串，方便存储到ETCD
func (l *IPFSLock) GetTargetString(target any) (string, error) {
	tar := target.(StringLockTarget)
	return StringLockTargetToString(&tar)
}

// ParseTargetString 解析字符串格式的锁对象数据
func (l *IPFSLock) ParseTargetString(targetStr string) (any, error) {
	return StringLockTargetFromString(targetStr)
}

// Clear 清除内部所有状态
func (l *IPFSLock) Clear() {
	l.nodeLocks = make(map[string]*IPFSNodeLock)
}

type IPFSNodeLock struct {
	buzyReqIDs []string
	gcReqIDs   []string

	lockCompatibilityTable *LockCompatibilityTable
}

func NewIPFSNodeLock() *IPFSNodeLock {
	compTable := &LockCompatibilityTable{}

	ipfsLock := IPFSNodeLock{
		lockCompatibilityTable: compTable,
	}

	compTable.
		Column(IPFSBuzyLock, func() bool { return len(ipfsLock.buzyReqIDs) > 0 }).
		Column(IPFSGCLock, func() bool { return len(ipfsLock.gcReqIDs) > 0 })

	comp := LockCompatible()
	uncp := LockUncompatible()

	compTable.MustRow(comp, uncp)
	compTable.MustRow(uncp, comp)

	return &ipfsLock
}

// CanLock 判断这个锁能否锁定成功
func (l *IPFSNodeLock) CanLock(lock distlock.Lock) error {
	return l.lockCompatibilityTable.Test(lock)
}

// 锁定
func (l *IPFSNodeLock) Lock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case IPFSBuzyLock:
		l.buzyReqIDs = append(l.buzyReqIDs, reqID)
	case IPFSGCLock:
		l.gcReqIDs = append(l.gcReqIDs, reqID)
	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}

// 解锁
func (l *IPFSNodeLock) Unlock(reqID string, lock distlock.Lock) error {
	switch lock.Name {
	case IPFSBuzyLock:
		l.buzyReqIDs = lo2.Remove(l.buzyReqIDs, reqID)
	case IPFSGCLock:
		l.gcReqIDs = lo2.Remove(l.gcReqIDs, reqID)
	default:
		return fmt.Errorf("unknow lock name: %s", lock.Name)
	}

	return nil
}
