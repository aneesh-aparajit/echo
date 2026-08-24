package echoserver

import "log/slog"

type EchoServer struct {
	logger *slog.Logger
}

func NewEchoServer(logger *slog.Logger) *EchoServer {
	return &EchoServer{
		logger: logger,
	}
}
