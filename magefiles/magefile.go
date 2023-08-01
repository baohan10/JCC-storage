//go:build mage

package main

import (
	"magefiles"

	//mage:import
	_ "magefiles/targets"
)

var Default = Build

func Build() error {
	return magefiles.Build(magefiles.BuildArgs{
		OutputName: "scanner",
		OutputDir:  "../../build/scanner",
		AssetsDir:  "assets",
	})
}
