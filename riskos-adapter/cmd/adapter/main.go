package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/riskos-adapter/internal/adapter"
	"github.com/example/riskos-adapter/internal/bus"
	"github.com/example/riskos-adapter/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := adapter.ConfigFromEnv()

	src, err := store.Open(cfg.FeatureStoreDSN)
	if err != nil {
		logger.Error("feature store open", "err", err)
		os.Exit(1)
	}
	defer src.Close()

	pub, err := bus.Dial(cfg.BusURL, cfg.BusSubject)
	if err != nil {
		logger.Error("bus dial", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	a := adapter.New(src, pub, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		logger.Info("shutdown requested")
		cancel()
	}()

	if err := a.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("adapter run", "err", err)
		os.Exit(1)
	}
	// Give the bus a moment to flush.
	time.Sleep(200 * time.Millisecond)
	logger.Info("shutdown complete")
}
