package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const monitor = "bluez_output.84_9D_4B_82_7E_4B.1.monitor"

func main() {
	// var (
	// 	port = flag.String("port", "8443", "Port to listen on")
	// )
	// flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stdout, cmd, err := StartCapture(ctx, monitor)
	if err != nil {
		log.Fatalf("start: %v", err)
	}

	defer stdout.Close()
	defer cmd.Wait()

	// push 4096 byte block to hub broadcast channel
	err = ReadChunks(stdout, 4096, func(b []byte) error {
		log.Printf("got %d bytes", len(b))
		return nil
	})

	log.Printf("ended: %v", err)
}
