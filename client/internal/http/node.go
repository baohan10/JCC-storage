package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type NodeService struct {
	*Server
}

func (s *Server) NodeSvc() *NodeService {
	return &NodeService{
		Server: s,
	}
}

type GetNodesReq struct {
	NodeIDs *[]cdssdk.NodeID `form:"nodeIDs" binding:"required"`
}
type GetNodesResp = cdssdk.NodeGetNodesResp

func (s *ObjectService) GetNodes(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Node.GetNodes")

	var req GetNodesReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodes, err := s.svc.NodeSvc().GetNodes(*req.NodeIDs)
	if err != nil {
		log.Warnf("getting nodes: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get nodes failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(GetNodesResp{Nodes: nodes}))
}
