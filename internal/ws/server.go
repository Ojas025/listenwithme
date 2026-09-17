package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ch <- v waits for a room
// <-ch waits for a value
// select with default turns either into - try without waiting

type Server struct {
	mu   sync.Mutex
	conn *websocket.Conn

	send      chan []byte
	connected chan bool

	http *http.Server

	ctx    context.Context
	cancel context.CancelFunc

	writeTimeout time.Duration

	droppedChunks int
}

func (s *Server) ListenerConnected() <-chan bool {
	return s.connected
}

func NewServer(ctx context.Context, queueSize int, writeTimeout time.Duration) *Server {
	if queueSize <= 0 {
		queueSize = 64
	}

	ctx, cancel := context.WithCancel(ctx)

	return &Server{
		send:          make(chan []byte, queueSize),
		connected:     make(chan bool, 1),
		ctx:           ctx,
		cancel:        cancel,
		writeTimeout:  writeTimeout,
		droppedChunks: 0,
		conn:          nil,
	}
}

// start the server, define route, listen for incoming connections
func (s *Server) Start(port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleAccept)

	s.http = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// each connection creates a separate drain instance
	// replace underlying connection, keeping one persistent drainer
	// start the drainer lifetime once, listener joining only updates the state
	go s.writeAudio()

	err := s.http.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (s *Server) Notify(notif bool) {
	placed := false
	// Coalescing signal pattern
	// loop until latest notif is placed into the channel
	for placed != true {
		// place fresh notif without waiting
		select {
		case s.connected <- notif:
			placed = true
		default:
		}

		if placed {
			break
		}

		// try notif eviction
		select {
		case <-s.connected:
		default:
		}
	}
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

	s.Notify(true)

	if old != nil {
		_ = old.Close(
			websocket.StatusPolicyViolation,
			"replaced by new connection",
		)
	}
}

func (s *Server) WriteChunk(b []byte) error {
	if len(b) == 0 {
		return errors.New("empty chunk")
	}

	// copy chunk so queued audio doesn't corrupt
	c := make([]byte, len(b))
	copy(c, b)
	b = c

	// try sending
	select {
	case s.send <- b:
		return nil
	default:
	}

	// evict to make space
	select {
	case <-s.send:
		s.mu.Lock()
		s.droppedChunks += 1
		s.mu.Unlock()
	default:
	}

	// send else drop latest
	select {
	case s.send <- b:
		return nil
	default:
		s.mu.Lock()
		s.droppedChunks += 1
		s.mu.Unlock()
	}

	return nil
}

func (s *Server) writeAudio() {
	var l *websocket.Conn

	for {
		select {
		case <-s.ctx.Done():
			return

		case audio := <-s.send:
			s.mu.Lock()
			l = s.conn
			s.mu.Unlock()

			if l == nil {
				continue
			}

			// send audio chunks over the connection
			ctx, cancel := context.WithTimeout(s.ctx, s.writeTimeout)

			err := l.Write(
				ctx,
				websocket.MessageBinary,
				audio,
			)

			cancel()

			if err != nil {
				s.clearConnection(l)
				continue
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

	s.Notify(false)

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
