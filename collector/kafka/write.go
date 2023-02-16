package kafka

import (
	"context"
	"log"

	kafkago "github.com/segmentio/kafka-go"
)

type Writer struct {
	Writer []*kafkago.Writer
}

func NewKafkaWriter() *Writer {
	brokers := []string{"localhost:9092"}
	writers := make([]*kafkago.Writer, len(brokers))
	for i, broker := range brokers {
		writers[i] = &kafkago.Writer{
			Addr:                   kafkago.TCP(broker),
			Topic:                  "logs",
			AllowAutoTopicCreation: true,
			Balancer:               &kafkago.LeastBytes{},
		}
	}

	return &Writer{
		Writer: writers,
	}
}

func (k *Writer) WriteMessages(ctx context.Context, logEvent []byte) error {

	messages := kafkago.Message{}

	messages.Value = logEvent

	for _, writer := range k.Writer {
		err := writer.WriteMessages(ctx,
			kafkago.Message{
				Key:       nil,
				Value:     messages.Value,
				Partition: 0,
			},
		)
		if err != nil {
			log.Fatalf("Error writing message: %v", err)
		}
	}
	// Close all writers
	//for _, writer := range k.Writer {
	//	err := writer.Close()
	//	if err != nil {
	//		log.Fatalf("Error closing writer: %v", err)
	//	}
	//}

	return nil
}
