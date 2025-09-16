package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"inmem-db/internal/app"
	"inmem-db/internal/config"
)

func main() {
	cfg := config.MustLoad()
	a, err := app.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err = a.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
