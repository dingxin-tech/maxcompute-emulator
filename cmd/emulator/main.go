package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
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
	ttl := flag.Duration("session-ttl", 30*time.Minute, "data transfer session lifetime")
	maxSessions := flag.Int("max-sessions", 64, "maximum sessions per transfer API")
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
	s := &http.Server{Addr: *listen, Handler: server.New(e, server.Config{Project: *project, PublicEndpoint: *public, SessionTTL: *ttl, MaxSessions: *maxSessions}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(done)
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		slog.Error("listen", "error", err)
		os.Exit(1)
	}
	slog.Info("emulator ready", "version", server.Version, "listen", *listen)
	if err = serveUntilSignal(s, listener, done); err != nil {
		slog.Error("http", "error", err)
		os.Exit(1)
	}
}

func serveUntilSignal(s *http.Server, listener net.Listener, signals <-chan os.Signal) error {
	served := make(chan error, 1)
	go func() { served <- s.Serve(listener) }()
	select {
	case err := <-served:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	case <-signals:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Shutdown closes the listener first; wait here for active handlers before
		// main closes DuckDB or exits the process.
		if err := s.Shutdown(ctx); err != nil {
			s.Close()
			<-served
			return err
		}
		<-served
		return nil
	}
}
