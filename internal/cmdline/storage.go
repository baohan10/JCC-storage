package cmdline

import "gitlink.org.cn/cloudream/client/internal/services"

func (c *Commandline) MoveObjectToStorage(objectID int, storageID int) error {
	return services.StorageSvc(c.svc).MoveObjectToStorage(0, objectID, storageID)
}
