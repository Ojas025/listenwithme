package playback

import (
	"io"

	"github.com/ebitengine/oto/v3"
)

// Given a raw PCM []byte from client,
// send them continously to system audio output

type Player struct {
	ctx  *oto.Context
	oto  *oto.Player
	pipe *io.PipeWriter
}

func NewPlayer() (*Player, error) {
	ctx, ready, err := oto.NewContext(&oto.NewContextOptions{
		SampleRate:   48000,
		ChannelCount: 2,
		Format:       oto.FormatSignedInt16LE,
	})
	if err != nil {
		return nil, err
	}

	// wait until audio device/backend is ready
	<-ready

	reader, writer := io.Pipe()

	player := ctx.NewPlayer(reader)
	player.Play()

	return &Player{
		ctx:  ctx,
		oto:  player,
		pipe: writer,
	}, nil
}

func (p *Player) Write(audio []byte) error {
	_, err := p.pipe.Write(audio)
	return err
}

func (p *Player) Close() {
	p.pipe.Close()
}
