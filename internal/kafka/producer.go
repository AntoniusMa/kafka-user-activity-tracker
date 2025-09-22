package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Producer interface {
	PublishJSON(ctx context.Context, topic, key string, msgs ...any) error
	Close() error
}

type producer struct {
	writer KafkaWriter
}

func NewProducer(brokers []string) Producer {
	writer := kafka.Writer{Addr: kafka.TCP(brokers...)}
	return newProducer(&writer)
}

func newProducer(writer KafkaWriter) Producer {
	return &producer{
		writer: writer,
	}
}

func (p *producer) PublishJSON(ctx context.Context, topic, key string, msgs ...any) error {

	for _, msg := range msgs {
		data, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal json: %w", err)
		}
		err = p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: data})
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *producer) Close() error {
	err := p.writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}
