package lockprovider

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
)

const (
	LOCK_COMPATIBILITY_COMPATIBLE   LockCompatibilityType = "Compatible"
	LOCK_COMPATIBILITY_UNCOMPATIBLE LockCompatibilityType = "Uncompatible"
	LOCK_COMPATIBILITY_SPECIAL      LockCompatibilityType = "Special"
)

type HasSuchLockFn = func() bool

// LockCompatibilitySpecialFn 判断锁与指定的锁名是否兼容
type LockCompatibilitySpecialFn func(lock distlock.Lock, testLockName string) bool

type LockCompatibilityType string

type LockCompatibility struct {
	Type      LockCompatibilityType
	SpecialFn LockCompatibilitySpecialFn
}

func LockCompatible() LockCompatibility {
	return LockCompatibility{
		Type: LOCK_COMPATIBILITY_COMPATIBLE,
	}
}

func LockUncompatible() LockCompatibility {
	return LockCompatibility{
		Type: LOCK_COMPATIBILITY_UNCOMPATIBLE,
	}
}

func LockSpecial(specialFn LockCompatibilitySpecialFn) LockCompatibility {
	return LockCompatibility{
		Type:      LOCK_COMPATIBILITY_SPECIAL,
		SpecialFn: specialFn,
	}
}

type LockCompatibilityTableRow struct {
	LockName        string
	HasSuchLockFn   HasSuchLockFn
	Compatibilities []LockCompatibility
}

type LockCompatibilityTable struct {
	rows     []LockCompatibilityTableRow
	rowIndex int
}

func (t *LockCompatibilityTable) Column(lockName string, hasSuchLock HasSuchLockFn) *LockCompatibilityTable {
	t.rows = append(t.rows, LockCompatibilityTableRow{
		LockName:      lockName,
		HasSuchLockFn: hasSuchLock,
	})

	return t
}
func (t *LockCompatibilityTable) MustRow(comps ...LockCompatibility) {
	err := t.Row(comps...)
	if err != nil {
		panic(fmt.Sprintf("build lock compatibility table failed, err: %s", err.Error()))
	}
}

func (t *LockCompatibilityTable) Row(comps ...LockCompatibility) error {
	if t.rowIndex >= len(t.rows) {
		return fmt.Errorf("there should be no more rows in the table")
	}

	if len(comps) < len(t.rows) {
		return fmt.Errorf("the columns should equals the rows")
	}

	t.rows[t.rowIndex].Compatibilities = comps

	for i := 0; i < t.rowIndex-1; i++ {
		chkRowCeil := t.rows[t.rowIndex].Compatibilities[i]
		chkColCeil := t.rows[i].Compatibilities[t.rowIndex]

		if chkRowCeil.Type != chkColCeil.Type {
			return fmt.Errorf("value at %d, %d is not equals to at %d, %d", t.rowIndex, i, i, t.rowIndex)
		}
	}

	t.rowIndex++

	return nil
}

func (t *LockCompatibilityTable) Test(lock distlock.Lock) error {
	row, ok := lo.Find(t.rows, func(row LockCompatibilityTableRow) bool { return lock.Name == row.LockName })
	if !ok {
		return fmt.Errorf("unknow lock name %s", lock.Name)
	}

	for i, c := range row.Compatibilities {
		if c.Type == LOCK_COMPATIBILITY_COMPATIBLE {
			continue
		}

		if c.Type == LOCK_COMPATIBILITY_UNCOMPATIBLE {
			if t.rows[i].HasSuchLockFn() {
				return distlock.NewLockTargetBusyError(t.rows[i].LockName)
			}
		}

		if c.Type == LOCK_COMPATIBILITY_SPECIAL {
			if !c.SpecialFn(lock, t.rows[i].LockName) {
				return distlock.NewLockTargetBusyError(t.rows[i].LockName)
			}
		}
	}

	return nil
}
