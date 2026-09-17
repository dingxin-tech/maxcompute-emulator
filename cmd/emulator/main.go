package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	testMode := flag.Bool("test-mode", false, "enable bounded test fault API")
	testNetwork := flag.Bool("test-network", false, "explicitly allow test mode on a container/test network")
	quotas := flag.String("quotas", "", "comma separated configured quotas in addition to default")
	authMode := flag.String("auth-mode", "permissive", "permissive or strict")
	credentials := flag.String("auth-config", "", "JSON file of local test AK credentials and ACLs")
	logFormat := flag.String("log-format", "json", "json or text")
	flag.Parse()
	if *logFormat == "json" {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	} else if *logFormat != "text" {
		slog.Error("invalid log-format")
		os.Exit(1)
	}
	host, _, parseErr := net.SplitHostPort(*listen)
	if *testMode && !*testNetwork && (parseErr != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback()) {
		slog.Error("test-mode requires loopback or explicit --test-network")
		os.Exit(1)
	}
	if *authMode != "permissive" && *authMode != "strict" {
		slog.Error("invalid auth-mode")
		os.Exit(1)
	}
	var creds map[string]server.Credential
	if *authMode == "strict" {
		b, err := os.ReadFile(*credentials)
		if err != nil || json.Unmarshal(b, &creds) != nil || len(creds) == 0 {
			slog.Error("strict mode requires valid local auth-config")
			os.Exit(1)
		}
		for ak, c := range creds {
			if ak == "" || c.Secret == "" {
				slog.Error("invalid local credential")
				os.Exit(1)
			}
		}
	}
	var quotaNames []string
	for _, q := range strings.Split(*quotas, ",") {
		if q = strings.TrimSpace(q); q != "" {
			quotaNames = append(quotaNames, q)
		}
	}
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
	s := &http.Server{Addr: *listen, Handler: server.New(e, server.Config{TestMode: *testMode, Quotas: quotaNames, AuthMode: *authMode, Credentials: creds, Project: *project, PublicEndpoint: *public, SessionTTL: *ttl, MaxSessions: *maxSessions}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second}
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
