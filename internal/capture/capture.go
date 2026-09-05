package capture

import (
	"context"
	"io"
	"os"
	"os/exec"
)

// Run two separate processes - GO and ffmpeg
// ffmpeg writes bytes from the monitor, Go reads from it
// StartCapture returns the read end of the stdout pipe, does not start reading
func StartCapture(ctx context.Context, monitor string) (io.ReadCloser, *exec.Cmd, error) {

	// build the command struct
	// link ffmpeg lifetime to ctx
	// with exec.Command, Go can exit while ffmpeg still running
	// '-' outputs to stdout, required for pipe reading
	cmd := exec.CommandContext(ctx, "ffmpeg", "-f", "pulse", "-i", monitor, "-c:a", "pcm_s16le", "-ar", "48000", "-ac", "2", "-f", "s16le", "-")

	// print logs, errors to terminal, avoid blocking
	cmd.Stderr = os.Stderr

	// unidirectional message queue where ffmpeg writes, and Go can read using stdout
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, nil, err
	}

	return stdout, cmd, nil
}

func ReadChunks(r io.Reader, chunkSize int, yield func([]byte) error) error {
	buf := make([]byte, chunkSize)

	for {
		n, err := io.ReadFull(r, buf)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err := yield(buf[:n])
				if err != nil {
					return err
				}

				return nil
			}

			if err == io.EOF {
				return nil
			}

			return err
		}

		if n > 0 {
			err := yield(buf[:n])
			if err != nil {
				return err
			}
		}
	}
}
