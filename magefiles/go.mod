module magefiles

go 1.20

replace gitlink.org.cn/cloudream/common v0.0.0 => ../../common

require (
	github.com/magefile/mage v1.15.0
	github.com/otiai10/copy v1.12.0
	gitlink.org.cn/cloudream/common v0.0.0
)

require golang.org/x/sys v0.6.0 // indirect
