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
		OutputName: "agent",
		OutputDir:  "../../build/agent",
		AssetsDir:  "assets",
	})
}
