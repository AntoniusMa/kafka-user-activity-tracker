package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-activity-tracker/domain"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer interface {
	ConsumeMessages(ctx context.Context, handler MessageHandler) error
	Close() error
}

type MessageHandler func(event *domain.UserEvent) error

type consumer struct {
	reader KafkaReader
	topic  string
}

func NewConsumer(brokers []string, groupID, topic string) Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})

	return newConsumer(reader, topic)
}

func newConsumer(reader KafkaReader, topic string) Consumer {
	return &consumer{
		reader: reader,
		topic:  topic,
	}
}

func (c *consumer) ConsumeMessages(ctx context.Context, handler MessageHandler) error {
	log.Printf("Starting consumer for topic: %s", c.topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Consumer for topic %s stopped", c.topic)
			return ctx.Err()
		default:
			message, err := c.reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Error fetching message from topic %s: %v", c.topic, err)
				continue
			}

			event, err := unmarshalUserEvent(message.Value)
			if err != nil {
				log.Printf("Error unmarshaling message from topic %s: %v", c.topic, err)
				continue
			}

			err = handler(event)
			if err != nil {
				log.Printf("Error handling event from topic %s: %v", c.topic, err)
				continue
			}

			err = c.reader.CommitMessages(ctx, message)
			if err != nil {
				log.Printf("Error committing message from topic %s: %v", c.topic, err)
			}
		}
	}
}

func (c *consumer) Close() error {
	err := c.reader.Close()
	if err != nil {
		return fmt.Errorf("failed to close reader for topic %s: %w", c.topic, err)
	}
	return nil
}

func unmarshalUserEvent(data []byte) (*domain.UserEvent, error) {
	var event domain.UserEvent
	err := json.Unmarshal(data, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal user event: %w", err)
	}
	return &event, nil
}
