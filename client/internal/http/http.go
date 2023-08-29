package http

import "gitlink.org.cn/cloudream/common/consts/errorcode"

type Response struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func OK(data any) Response {
	return Response{
		Code:    errorcode.OK,
		Message: "",
		Data:    data,
	}
}

func Failed(code string, msg string) Response {
	return Response{
		Code:    code,
		Message: msg,
	}
}
