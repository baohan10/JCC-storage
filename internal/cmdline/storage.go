package cmdline

import "gitlink.org.cn/cloudream/client/internal/services"

func (c *Commandline) Move(objectID int, storageID int) error {
	return services.StorageSvc(c.svc).MoveObjectToStorage(0, objectID, storageID)
}
