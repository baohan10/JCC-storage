//go:build mage

package main

import (
	"path/filepath"

	"github.com/magefile/mage/sh"
)

func Protos() error {
	return proto("pkgs/grpc/agent", "agent.proto")
}

func proto(dir string, fileName string) error {
	return sh.Run("protoc", "--go_out="+dir, "--go-grpc_out="+dir, filepath.Join(dir, fileName))
}
