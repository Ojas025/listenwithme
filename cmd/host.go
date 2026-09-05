package cmd

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

func HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	defer conn.CloseNow()

	ctx := conn.CloseRead(r.Context())

	for {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, msg, err := conn.Read(ctx)
		cancel()
		if err != nil {
			return
		}

		err = conn.Write(ctx, websocket.MessageText, msg)
		if err != nil {
			return
		}
	}
}
