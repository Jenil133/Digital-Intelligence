package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/di-collector/internal/api"
	"github.com/example/di-collector/internal/redisx"
	"github.com/example/di-collector/internal/tracing"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := envOr("LISTEN_ADDR", ":8080")
	redisURL := envOr("REDIS_URL", "redis://localhost:6379/0")

	tracingShutdown, err := tracing.Setup(context.Background(), "di-collector")
	if err != nil {
		logger.Error("tracing init", "err", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracingShutdown(ctx)
	}()

	rdb, err := redisx.New(redisURL)
	if err != nil {
		logger.Error("redis init", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewRouter(rdb),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("shutdown complete")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
