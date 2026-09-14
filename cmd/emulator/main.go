package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/server"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	db := flag.String("database", "", "DuckDB file; empty for temporary data")
	seed := flag.String("seed", "", "ODPS SQL fixture file")
	project := flag.String("project", "test_project", "fixture project")
	schema := flag.String("schema", "default", "fixture schema")
	public := flag.String("public-endpoint", "", "advertised endpoint; default request Host")
	ttl := flag.Duration("session-ttl", 30*time.Minute, "download session lifetime")
	maxRows := flag.Int("max-rows", 100000, "maximum rows per result/session")
	flag.Parse()
	e, err := engine.Open(*db, *maxRows)
	if err != nil {
		slog.Error("engine", "error", err)
		os.Exit(1)
	}
	defer e.Close()
	if *seed != "" {
		b, err := os.ReadFile(*seed)
		if err == nil {
			_, err = e.Execute(context.Background(), *project, *schema, string(b))
		}
		if err != nil {
			slog.Error("seed failed", "error", err)
			os.Exit(1)
		}
	}
	s := &http.Server{Addr: *listen, Handler: server.New(e, server.Config{PublicEndpoint: *public, SessionTTL: *ttl}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	}()
	slog.Info("emulator ready", "version", server.Version, "listen", *listen)
	if err = s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http", "error", err)
		os.Exit(1)
	}
}
