package cmdline

func StorageMoveObjectToStorage(ctx CommandContext, objectID int, storageID int) error {
	return ctx.Cmdline.Svc.StorageSvc().MoveObjectToStorage(0, objectID, storageID)
}

func init() {
	commands.MustAdd(StorageMoveObjectToStorage, "storage", "move")
}
