//go:build mage

package main

import (
	"gitlink.org.cn/cloudream/common/magefiles"

	//mage:import
	_ "gitlink.org.cn/cloudream/common/magefiles/targets"
)

var Default = Build

func Build() error {
	return magefiles.Build(magefiles.BuildArgs{
		OutputName: "scanner",
		OutputDir:  "scanner",
		AssetsDir:  "assets",
	})
}
