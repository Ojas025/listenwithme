package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ojas025/listenwithme/cmd"
	"github.com/Ojas025/listenwithme/internal/config"
)

func main() {
	// var (
	// 	port = flag.String("port", "8443", "Port to listen on")
	// )
	// flag.Parse()

	config := config.LoadConfig()

	ctx := context.Background()
	ctx = context.WithValue(ctx, "config", config)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)

	defer stop()

	err := cmd.RootCmd.ExecuteContext(ctx)
	if err != nil {
		os.Exit(1)
	}
}
