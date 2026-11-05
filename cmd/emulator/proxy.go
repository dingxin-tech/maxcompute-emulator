package main

import (
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/captureproxy"
)

func runProxy(args []string) error {
	flags := flag.NewFlagSet("emulator proxy", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	upstream := flags.String("upstream", "", "fixed upstream HTTP(S) endpoint, including any base path")
	headers := flags.String("capture-headers", "", "comma-separated request header names to include in JSON Lines")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("proxy accepts flags only")
	}
	handler, err := captureproxy.New(*upstream, strings.Split(*headers, ","), os.Stdout)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	slog.Info("proxy ready", "listen", listener.Addr().String())
	return serveUntilSignal(server, listener, signals)
}
