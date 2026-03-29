package sim

import (
	"fmt"
	"log/slog"
	"os"
)

const DefaultListenPort = 8000

func getLogLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "DEBUG", "debug":
		return slog.LevelDebug
	case "INFO", "info":
		return slog.LevelInfo
	case "WARN", "warn":
		return slog.LevelWarn
	case "ERROR", "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func newNodeLogger(nodeNum int) *slog.Logger {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     getLogLevel(),
	}))
	return logger.With("peer", fmt.Sprintf("%d", nodeNum)).With("node-num", nodeNum)
}
