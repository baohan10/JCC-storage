package distlock

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/trie"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type Service = distlock.Service

func NewService(cfg *distlock.Config) (*distlock.Service, error) {
	srv, err := distlock.NewService(cfg, initProviders())
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func initProviders() []distlock.PathProvider {
	var provs []distlock.PathProvider

	provs = append(provs, initMetadataLockProviders()...)

	provs = append(provs, initIPFSLockProviders()...)

	provs = append(provs, initStorageLockProviders()...)

	return provs
}

func initMetadataLockProviders() []distlock.PathProvider {
	return []distlock.PathProvider{
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Node"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Storage"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "User"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "UserBucket"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "UserNode"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "UserStorage"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Bucket"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Object"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Package"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "ObjectRep"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "ObjectBlock"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Cache"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "StoragePackage"),
		distlock.NewPathProvider(lockprovider.NewMetadataLock(), lockprovider.MetadataLockPathPrefix, "Location"),
	}
}

func initIPFSLockProviders() []distlock.PathProvider {
	return []distlock.PathProvider{
		distlock.NewPathProvider(lockprovider.NewIPFSLock(), lockprovider.IPFSLockPathPrefix, trie.WORD_ANY),
	}
}

func initStorageLockProviders() []distlock.PathProvider {
	return []distlock.PathProvider{
		distlock.NewPathProvider(lockprovider.NewStorageLock(), lockprovider.StorageLockPathPrefix, trie.WORD_ANY),
	}
}
