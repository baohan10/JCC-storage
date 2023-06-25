//go:build mage

package main

import (
	"magefiles"
	"magefiles/utils"

	//mage:import
	"magefiles/targets"
)

var Default = Build

func Build() error {
	return utils.Build(magefiles.BuildArgs{
		OutputBinName: "scanner",
		OutputDirName: "scanner",
		AssetsDir:     "assets",
		PubArgs:       targets.PubGoBuildArgs,
	})
}
