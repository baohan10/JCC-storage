package stgglb

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

// 根据当前节点与目标地址的距离关系，选择合适的地址
func SelectGRPCAddress(node *cdssdk.Node) (string, int) {
	if Local != nil && Local.LocationID == node.LocationID {
		return node.LocalIP, node.LocalGRPCPort
	}

	return node.ExternalIP, node.ExternalGRPCPort
}
