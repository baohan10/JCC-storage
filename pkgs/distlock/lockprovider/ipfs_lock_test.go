package lockprovider

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
)

func Test_IPFSLock(t *testing.T) {
	cases := []struct {
		title     string
		initLocks []distlock.Lock
		doLock    distlock.Lock
		wantOK    bool
	}{
		{
			title: "同节点，同一个Read锁",
			initLocks: []distlock.Lock{
				{
					Path: []string{IPFSLockPathPrefix, "node1"},
					Name: IPFS_SET_READ_LOCK,
				},
			},
			doLock: distlock.Lock{
				Path: []string{IPFSLockPathPrefix, "node1"},
				Name: IPFS_SET_READ_LOCK,
			},
			wantOK: true,
		},
		{
			title: "同节点，同一个Write锁",
			initLocks: []distlock.Lock{
				{
					Path: []string{IPFSLockPathPrefix, "node1"},
					Name: IPFS_SET_WRITE_LOCK,
				},
			},
			doLock: distlock.Lock{
				Path: []string{IPFSLockPathPrefix, "node1"},
				Name: IPFS_SET_WRITE_LOCK,
			},
			wantOK: false,
		},
		{
			title: "不同节点，同一个Write锁",
			initLocks: []distlock.Lock{
				{
					Path: []string{IPFSLockPathPrefix, "node1"},
					Name: IPFS_SET_WRITE_LOCK,
				},
			},
			doLock: distlock.Lock{
				Path: []string{IPFSLockPathPrefix, "node2"},
				Name: IPFS_SET_WRITE_LOCK,
			},
			wantOK: true,
		},
		{
			title: "相同对象的Read、Write锁",
			initLocks: []distlock.Lock{
				{
					Path:   []string{IPFSLockPathPrefix, "node1"},
					Name:   IPFS_ELEMENT_WRITE_LOCK,
					Target: *NewStringLockTarget(),
				},
			},
			doLock: distlock.Lock{
				Path:   []string{IPFSLockPathPrefix, "node1"},
				Name:   IPFS_ELEMENT_WRITE_LOCK,
				Target: *NewStringLockTarget(),
			},
			wantOK: false,
		},
	}

	for _, ca := range cases {
		Convey(ca.title, t, func() {
			ipfsLock := NewIPFSLock()

			for _, l := range ca.initLocks {
				ipfsLock.Lock("req1", l)
			}

			err := ipfsLock.CanLock(ca.doLock)
			if ca.wantOK {
				So(err, ShouldBeNil)
			} else {
				So(err, ShouldNotBeNil)
			}
		})
	}

	Convey("解锁", t, func() {
		ipfsLock := NewIPFSLock()

		lock := distlock.Lock{
			Path: []string{IPFSLockPathPrefix, "node1"},
			Name: IPFS_SET_WRITE_LOCK,
		}

		ipfsLock.Lock("req1", lock)

		err := ipfsLock.CanLock(lock)
		So(err, ShouldNotBeNil)

		ipfsLock.Unlock("req1", lock)

		err = ipfsLock.CanLock(lock)
		So(err, ShouldBeNil)
	})

}
