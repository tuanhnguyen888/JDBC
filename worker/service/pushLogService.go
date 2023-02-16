package service

import (
	"context"
	kafkago "github.com/segmentio/kafka-go"
	"worker/kafka"
	"worker/repository"
)

type PushLogService interface {
	PushLogToEs(ctx context.Context) error
}

type pushLogService struct {
	reader    kafka.Reader
	messages  chan kafkago.Message
	esLogRepo repository.EsLogRepository
}

func NewPushLogService(
	reader kafka.Reader,
	messages chan kafkago.Message,
	esLogRepository repository.EsLogRepository,
) PushLogService {
	return &pushLogService{
		reader:    reader,
		messages:  messages,
		esLogRepo: esLogRepository,
	}
}

func (w *pushLogService) PushLogToEs(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-w.messages:
			err := w.esLogRepo.SaveLog(m.Value)
			if err != nil {
				return err
			}
		}
	}
}
