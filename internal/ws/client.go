package ws

import (
	"context"
	"errors"

	"github.com/coder/websocket"
)

type Client struct {
	conn *websocket.Conn
}

func NewClient(ctx context.Context, address string) (*Client, error) {
	conn, _, err := websocket.Dial(ctx, address, nil)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn: conn,
	}, nil
}

func (c *Client) ReadAudio(ctx context.Context) ([]byte, error) {
	msgType, audio, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}

	if msgType == websocket.MessageBinary {
		return audio, nil
	}

	return nil, errors.New("invalid message type")
}

func (c *Client) Close() error {
	err := c.conn.Close(websocket.StatusNormalClosure, "client leaving")
	if err != nil {
		return err
	}

	return nil
}
