package cmd

// create a child context per session and cancel only that

import (
	"context"
	"fmt"
	"time"

	"github.com/Ojas025/listenwithme/internal/capture"
	"github.com/Ojas025/listenwithme/internal/config"
	"github.com/Ojas025/listenwithme/internal/ws"
	"github.com/spf13/cobra"
)

var hostCmd = &cobra.Command{
	Use:   "host",
	Short: "start hosting audio",
	RunE:  orchestrateHost,
}

func init() {
	hostCmd.Flags().StringVar(&port, "port", "8080", "p")
	RootCmd.AddCommand(hostCmd)
}

func StartReading(server *ws.Server, ctx context.Context, cfg config.Config, errChan chan<- error) {
	defer close(errChan)

	// listener connected, start capture
	stdout, proc, err := capture.StartCapture(ctx, cfg.Monitor)
	if err != nil {
		errChan <- err
		return
	}
	defer stdout.Close()

	err = capture.ReadChunks(stdout, cfg.ChunkSize, server.WriteChunk)
	if err != nil {
		proc.Cancel()
		proc.Wait()
		errChan <- err
		return
	}

	proc.Wait()
	errChan <- nil
}

func orchestrateHost(cmd *cobra.Command, args []string) error {
	cfg := cmd.Context().Value("config").(config.Config)
	cfg.Port = port

	// Create server instance
	server := ws.NewServer(cmd.Context(), cfg.QueueSize, 250*time.Millisecond)

	// Start server
	resultCh := make(chan error, 1)

	go func() {
		err := server.Start(cfg.Port)
		resultCh <- err
	}()

	var (
		cancel  context.CancelFunc
		errChan chan error
		ctx     context.Context
	)

	for {
		select {
		case <-cmd.Context().Done():
			// parent closed, stop current session gracefully
			fmt.Println("Host shutting down")

			if cancel != nil {
				cancel()
			}
			server.Close()
			return nil
		case err := <-resultCh:
			if err != nil {
				if cancel != nil {
					cancel()
				}
				return err
			}

		case connected := <-server.ListenerConnected():

			if connected {
				if cancel != nil {
					cancel()
					errChan = nil
					ctx = nil
					cancel = nil
				}

				ctx, cancel = context.WithCancel(cmd.Context())
				errChan = make(chan error, 1)
				go StartReading(server, ctx, cfg, errChan)

			} else {
				if cancel != nil {
					cancel()
				}
			}
		case err := <-errChan:
			if err != nil {
				if cancel != nil {
					cancel()
				}
			}

			errChan = nil
			cancel = nil
			ctx = nil
		}
	}
}
