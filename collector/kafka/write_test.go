package kafka_test

import (
	"collector/kafka"
	"context"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewKafkaWriter(t *testing.T) {
	k := kafka.NewKafkaWriter()

	// Kiểm tra độ dài của slice writer
	if len(k.Writer) != 1 {
		t.Errorf("Expected length of slice writer to be 1, but got %d", len(k.Writer))
	}

	// Kiểm tra các giá trị trong slice writer
	if k.Writer[0].Topic != "logs" {
		t.Errorf("Expected topic to be logs, but got %s", k.Writer[0].Topic)
	}

	if k.Writer[0].Addr.String() != "localhost:9092" {
		t.Errorf("Expected address to be localhost:9092, but got %s", k.Writer[0].Addr.String())
	}

	if _, ok := k.Writer[0].Balancer.(*kafkago.LeastBytes); !ok {
		t.Error("Expected balancer to be an instance of LeastBytes")
	}
}

func TestWriteMessages(t *testing.T) {
	// create a new kafka writer instance
	kafkaWriter := kafka.NewKafkaWriter()

	// set up some test data
	testData := []byte("test data")

	// create a new context
	ctx := context.Background()

	// call WriteMessages method to write test data to kafka
	err := kafkaWriter.WriteMessages(ctx, testData)
	assert.NoError(t, err, "unexpected error")
}
