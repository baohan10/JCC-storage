package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/storage-client/internal/http"
)

func ServeHTTP(ctx CommandContext, args []string) error {
	listenAddr := ":7890"
	if len(args) > 0 {
		listenAddr = args[0]
	}

	httpSvr, err := http.NewServer(listenAddr, ctx.Cmdline.Svc)
	if err != nil {
		return fmt.Errorf("new http server: %w", err)
	}

	err = httpSvr.Serve()
	if err != nil {
		return fmt.Errorf("serving http: %w", err)
	}

	return nil
}

func init() {
	commands.MustAdd(ServeHTTP, "serve", "http")
}
