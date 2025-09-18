package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

var basicTopics = []kafka.TopicConfig{
	{Topic: "user-logins", NumPartitions: 3},
	{Topic: "page-views", NumPartitions: 2},
	{Topic: "user-actions", NumPartitions: 1}}

type KafkaConn interface {
	CreateTopics(topics ...kafka.TopicConfig) error
	Close() error
}

type ConnDialer interface {
	DialContext(ctx context.Context, network, address string) (KafkaConn, error)
}

type DefaultDialer struct{}

func (d DefaultDialer) DialContext(ctx context.Context, network, address string) (KafkaConn, error) {
	return kafka.DialContext(ctx, network, address)
}

func createTopics(kafkaConnection KafkaConn, topics ...kafka.TopicConfig) error {
	return kafkaConnection.CreateTopics(topics...)
}

func initKafkaTopics(dialer ConnDialer, brokers []string) error {
	conn, err := dialer.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	err = createTopics(conn, basicTopics...)
	if err != nil {
		return err
	}

	log.Printf("Created topics:\n %v", basicTopics)

	return nil
}
