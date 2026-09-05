package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type Server struct {
	mu   sync.Mutex
	conn *websocket.Conn

	send chan []byte

	http *http.Server

	ctx    context.Context
	cancel context.CancelFunc

	writeTimeout time.Duration
}

func NewServer(ctx context.Context, queueSize int, writeTimeout time.Duration) *Server {
	if queueSize <= 0 {
		queueSize = 64
	}

	ctx, cancel := context.WithCancel(ctx)

	return &Server{
		send:         make(chan []byte, queueSize),
		ctx:          ctx,
		cancel:       cancel,
		writeTimeout: writeTimeout,
	}
}

// start the server, define route, listen for incoming connections
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleAccept)

	s.http = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	err := s.http.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// listener dials, upgrade the request to ws and store the conn
func (s *Server) handleAccept(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}

	s.mu.Lock()

	old := s.conn
	s.conn = conn

	s.mu.Unlock()

	if old != nil {
		_ = old.Close(
			websocket.StatusPolicyViolation,
			"replaced by new connection",
		)
	}

	go s.writeAudio(conn)
}

func (s *Server) writeAudio(conn *websocket.Conn) {
	for {
		select {
		case <-s.ctx.Done():
			return

		case audio := <-s.send:
			// send audio chunks over the connection
			ctx, cancel := context.WithTimeout(s.ctx, s.writeTimeout)

			err := conn.Write(
				ctx,
				websocket.MessageBinary,
				audio,
			)

			cancel()

			if err != nil {
				s.clearConnection(conn)
				return
			}
		}
	}
}

func (s *Server) clearConnection(conn *websocket.Conn) {
	s.mu.Lock()
	if s.conn == conn {
		s.conn = nil
	}
	s.mu.Unlock()

	_ = conn.Close(
		websocket.StatusAbnormalClosure,
		"failed peer connection",
	)
}

func (s *Server) Close() error {
	s.cancel()

	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()

	if conn != nil {
		conn.Close(websocket.StatusNormalClosure, "server shutting down")
	}

	if s.http != nil {
		s.http.Shutdown(context.Background())
	}

	return nil
}
