package cmdline

import (
	"fmt"
	"strings"

	"gitlink.org.cn/cloudream/common/pkg/distlock"
	"gitlink.org.cn/cloudream/common/pkg/distlock/lockprovider"
)

func DistLockLock(ctx CommandContext, lockData []string) error {
	req := distlock.LockRequest{}

	for _, lock := range lockData {
		l, err := parseOneLock(lock)
		if err != nil {
			return fmt.Errorf("parse lock data %s failed, err: %w", lock, err)
		}

		req.Locks = append(req.Locks, l)
	}

	reqID, err := ctx.Cmdline.DistLock.Acquire(req)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}

	fmt.Printf("%s\n", reqID)

	return nil
}

func parseOneLock(lockData string) (distlock.Lock, error) {
	var lock distlock.Lock

	fullPathAndTarget := strings.Split(lockData, "@")
	if len(fullPathAndTarget) != 2 {
		return lock, fmt.Errorf("lock data must contains lock path, name and target")
	}

	pathAndName := strings.Split(fullPathAndTarget[0], "/")
	if len(pathAndName) < 2 {
		return lock, fmt.Errorf("lock data must contains lock path, name and target")
	}

	lock.Path = pathAndName[0 : len(pathAndName)-1]
	lock.Name = pathAndName[len(pathAndName)-1]

	target := lockprovider.NewStringLockTarget()
	comps := strings.Split(fullPathAndTarget[1], "/")
	for _, comp := range comps {
		target.Add(strings.Split(comp, "."))
	}

	lock.Target = *target

	return lock, nil
}

func DistLockUnlock(ctx CommandContext, reqID string) error {
	return ctx.Cmdline.DistLock.Release(reqID)
}

func init() {
	commands.MustAdd(DistLockLock, "distlock", "lock")

	commands.MustAdd(DistLockUnlock, "distlock", "unlock")
}
