package main

import (
	"context"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"log"
	"worker/kafka"
	"worker/repository"
	"worker/service"
)

var (
	reader    = kafka.NewKafkaReader()
	messages  = make(chan kafkago.Message, 1000)
	esLogRepo = repository.NewEsLogRepository()

	logService = service.NewPushLogService(*reader, messages, esLogRepo)
)

func main() {
	ctx := context.Background()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return reader.FetchMessage(ctx, messages)
	})

	g.Go(func() error {
		return logService.PushLogToEs(ctx)
	})

	err := g.Wait()
	if err != nil {
		log.Fatalln(err)
	}
}
