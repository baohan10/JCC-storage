package scanner

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mymq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

// Service 协调端接口
type Service interface {
	EventService
}
type Server struct {
	service   Service
	rabbitSvr mq.RabbitMQServer

	OnError func(err error)
}

func NewServer(svc Service, cfg *mymq.Config) (*Server, error) {
	srv := &Server{
		service: svc,
	}

	rabbitSvr, err := mq.NewRabbitMQServer(
		cfg.MakeConnectingURL(),
		mymq.SCANNER_QUEUE_NAME,
		func(msg *mq.Message) (*mq.Message, error) {
			return msgDispatcher.Handle(srv.service, msg)
		},
	)
	if err != nil {
		return nil, err
	}

	srv.rabbitSvr = *rabbitSvr

	return srv, nil
}

func (s *Server) Stop() {
	s.rabbitSvr.Close()
}

func (s *Server) Serve() error {
	return s.rabbitSvr.Serve()
}

var msgDispatcher mq.MessageDispatcher = mq.NewMessageDispatcher()

// Register 将Service中的一个接口函数作为指定类型消息的处理函数，同时会注册请求和响应的消息类型
// TODO 需要约束：Service实现了TSvc接口
func Register[TReq mq.MessageBody, TResp mq.MessageBody](svcFn func(svc Service, msg TReq) (TResp, *mq.CodeMessage)) any {
	mq.AddServiceFn(&msgDispatcher, svcFn)
	mq.RegisterMessage[TReq]()
	mq.RegisterMessage[TResp]()

	return nil
}

// RegisterNoReply 将Service中的一个*没有返回值的*接口函数作为指定类型消息的处理函数，同时会注册请求和响应的消息类型
// TODO 需要约束：Service实现了TSvc接口
func RegisterNoReply[TReq mq.MessageBody](svcFn func(svc Service, msg TReq)) any {
	mq.AddNoRespServiceFn(&msgDispatcher, svcFn)
	mq.RegisterMessage[TReq]()

	return nil
}
