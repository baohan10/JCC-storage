package grpc

import (
	"context"

	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

func (s *Service) Ping(context.Context, *agtrpc.PingReq) (*agtrpc.PingResp, error) {
	return &agtrpc.PingResp{}, nil
}
