package distlock

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/pkgs/trie"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type Service = service.Service

func NewService(cfg *distlock.Config) (*service.Service, error) {
	srv, err := service.NewService(cfg, initProviders())
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func initProviders() []service.PathProvider {
	var provs []service.PathProvider

	provs = append(provs, initMetadataLockProviders()...)

	provs = append(provs, initIPFSLockProviders()...)

	provs = append(provs, initStorageLockProviders()...)

	return provs
}

func initMetadataLockProviders() []service.PathProvider {
	return []service.PathProvider{
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Node"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Storage"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "User"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "UserBucket"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "UserNode"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "UserStorage"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Bucket"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Object"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Package"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "ObjectRep"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "ObjectBlock"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Cache"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "StoragePackage"),
		service.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Location"),
	}
}

func initIPFSLockProviders() []service.PathProvider {
	return []service.PathProvider{
		service.NewPathProvider(lockprovider.NewIPFSLock(), lockprovider.IPFSLockPathPrefix, trie.WORD_ANY),
	}
}

func initStorageLockProviders() []service.PathProvider {
	return []service.PathProvider{
		service.NewPathProvider(lockprovider.NewStorageLock(), lockprovider.StorageLockPathPrefix, trie.WORD_ANY),
	}
}
