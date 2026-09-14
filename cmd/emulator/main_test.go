package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSignalWaitsForActiveRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	s := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		io.WriteString(w, "committed")
	})}
	defer s.Close()
	signals, served := make(chan os.Signal, 1), make(chan error, 1)
	go func() { served <- serveUntilSignal(s, listener, signals) }()
	response := make(chan string, 1)
	go func() {
		res, err := http.Get("http://" + listener.Addr().String())
		if err != nil {
			response <- err.Error()
			return
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		response <- string(b)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	signals <- syscall.SIGTERM
	select {
	case err := <-served:
		close(release)
		t.Fatalf("returned before active request finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if body := <-response; body != "committed" {
		t.Fatalf("response lost during shutdown: %s", body)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete")
	}
}
