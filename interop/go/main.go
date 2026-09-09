// A deliberately small authenticated local worker, not an Internet-facing service.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sync"
	"time"
)

type boundedListener struct {
	net.Listener
	slots chan struct{}
}
type boundedConn struct {
	net.Conn
	slots chan struct{}
	once  sync.Once
}

func (c *boundedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { <-c.slots })
	return err
}
func (l *boundedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &boundedConn{Conn: c, slots: l.slots}, nil
		default:
			_ = c.Close()
		}
	}
}

func handler(desc map[string]any, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		fail := func(code int) { w.WriteHeader(code); _, _ = io.WriteString(w, `{"error":"request_rejected"}`) }
		auth := r.Header.Values("Authorization")
		if len(auth) != 1 || subtle.ConstantTimeCompare([]byte(auth[0]), []byte("Bearer "+token)) != 1 {
			fail(401)
			return
		}
		if r.Header.Get("Origin") != "" || len(r.TransferEncoding) != 0 || r.Header.Get("Content-Encoding") != "" {
			fail(400)
			return
		}
		var response any
		if r.Method == "GET" && r.RequestURI == "/v1/info" && r.ContentLength == 0 {
			response = desc
		} else if r.Method == "POST" && r.RequestURI == "/v1/process" {
			if r.ContentLength < 0 || r.ContentLength > maxWire || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" {
				fail(400)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxWire)
			raw, err := io.ReadAll(r.Body)
			if err != nil || int64(len(raw)) != r.ContentLength {
				fail(400)
				return
			}
			value, err := decode(raw)
			if err != nil {
				fail(400)
				return
			}
			start := time.Now()
			result, err := processRequest(value, desc)
			if err != nil {
				fail(400)
				return
			}
			result["duration_ms"] = time.Since(start).Milliseconds()
			response = result
		} else {
			fail(404)
			return
		}
		raw, err := json.Marshal(response)
		if err != nil || len(raw) > maxWire {
			fail(413)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(raw)))
		w.WriteHeader(200)
		_, _ = w.Write(raw)
	})
}

func run() error {
	host := flag.String("host", "127.0.0.1", "literal loopback bind address")
	port := flag.Int("port", 0, "TCP port (0 chooses an available port)")
	env := flag.String("token-env", "CORPUSLEDGER_WORKER_TOKEN", "environment variable containing Bearer token")
	flag.Parse()
	if (*host != "127.0.0.1" && *host != "::1") || *port < 0 || *port > 65535 || flag.NArg() != 0 {
		return invalid
	}
	token := os.Getenv(*env)
	if !regexp.MustCompile(`^[A-Za-z0-9._~-]{32,256}$`).MatchString(token) {
		return invalid
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(exe)
	if err != nil {
		return err
	}
	config := digest([]byte("corpusledger.go-token-worker.v1\n" + digest(raw) + "\n" + runtime.Version() + "\n" + runtime.GOOS + "/" + runtime.GOARCH + "\nUnicode-White_Space-explicit;codepoint;no-normalization"))
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, fmt.Sprint(*port)))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler(description(config), token), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 3 * time.Second, MaxHeaderBytes: 8192, ErrorLog: log.New(io.Discard, "", 0)}
	server.SetKeepAlivesEnabled(false)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	go func() {
		<-ctx.Done()
		deadline, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		_ = server.Shutdown(deadline)
		_ = server.Close()
	}()
	// Only the bound address is printed. Tokens, input, and decoder errors never are.
	fmt.Printf("http://%s\n", listener.Addr())
	err = server.Serve(&boundedListener{listener, make(chan struct{}, 8)})
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "worker_start_or_run_failed")
		os.Exit(2)
	}
}
