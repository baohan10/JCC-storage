//go:build mage

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/magefile/mage/sh"
	cp "github.com/otiai10/copy"
)

const (
	BuildDir = "./build"
)

var Global = struct {
	OS   string
	Arch string
}{}

// [配置项]设置编译平台为windows
func Win() {
	Global.OS = "win"
}

// [配置项]设置编译平台为linux
func Linux() {
	Global.OS = "linux"
}

// [配置项]设置编译架构为amd64
func AMD64() {
	Global.Arch = "amd64"
}

func All() error {
	if err := Bin(); err != nil {
		return err
	}

	if err := Scripts(); err != nil {
		return err
	}

	if err := Confs(); err != nil {
		return err
	}

	return nil
}

func Bin() error {
	if err := Agent(); err != nil {
		return err
	}
	if err := Client(); err != nil {
		return err
	}
	if err := Coordinator(); err != nil {
		return err
	}
	if err := Scanner(); err != nil {
		return err
	}

	return nil
}

func Scripts() error {
	scriptsDir := "./storage-common/assets/scripts"

	info, err := os.Stat(scriptsDir)
	if errors.Is(err, os.ErrNotExist) || !info.IsDir() {
		return fmt.Errorf("script directory not exists or is not a directory")
	}

	fullDirPath, err := filepath.Abs(filepath.Join(BuildDir, "scripts"))
	if err != nil {
		return err
	}

	fmt.Printf("copying scripts to %s\n", fullDirPath)

	return cp.Copy(scriptsDir, fullDirPath)
}

func Confs() error {
	confDir := "./storage-common/assets/confs"

	info, err := os.Stat(confDir)
	if errors.Is(err, os.ErrNotExist) || !info.IsDir() {
		return fmt.Errorf("conf directory not exists or is not a directory")
	}

	fullDirPath, err := filepath.Abs(filepath.Join(BuildDir, "confs"))
	if err != nil {
		return err
	}

	fmt.Printf("copying confs to %s\n", fullDirPath)

	return cp.Copy(confDir, fullDirPath)
}

func Agent() error {
	os.Chdir("./storage-agent")
	defer os.Chdir("..")

	return sh.RunV("mage", makeBuildMageArgeuments()...)
}

func Client() error {
	os.Chdir("./storage-client")
	defer os.Chdir("..")

	return sh.RunV("mage", makeBuildMageArgeuments()...)
}

func Coordinator() error {
	os.Chdir("./storage-coordinator")
	defer os.Chdir("..")

	return sh.RunV("mage", makeBuildMageArgeuments()...)
}

func Scanner() error {
	os.Chdir("./storage-scanner")
	defer os.Chdir("..")

	return sh.RunV("mage", makeBuildMageArgeuments()...)
}

func makeBuildMageArgeuments() []string {
	var args []string

	if Global.OS != "" {
		args = append(args, Global.OS)
	}

	if Global.Arch != "" {
		args = append(args, Global.Arch)
	}

	args = append(args, "buildroot", "../build")

	args = append(args, "build")

	return args
}
