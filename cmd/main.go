package main

import (
	"context"
	"inmem-db/internal/app"
	"inmem-db/internal/config"
	"log"
	"os"
	"os/signal"
)

func main() {
	cfg := config.MustLoad()
	a := app.New(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err := a.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
