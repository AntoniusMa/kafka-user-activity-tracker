package main

import (
	"context"
	"database/sql"
	"fmt"
	"kafka-activity-tracker/config"
	"kafka-activity-tracker/domain"
	"log"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var basicTopics = []kafka.TopicConfig{
	{Topic: domain.EventTopicMap[domain.LOGIN], NumPartitions: 3},
	{Topic: domain.EventTopicMap[domain.PAGE_VIEWS], NumPartitions: 2},
	{Topic: domain.EventTopicMap[domain.USER_ACTION], NumPartitions: 1}}

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

type DBOpener interface {
	Open(driverName, dataSourceName string) (*sql.DB, error)
}

type DefaultDBOpener struct{}

func (d DefaultDBOpener) Open(driverName, dataSourceName string) (*sql.DB, error) {
	return sql.Open(driverName, dataSourceName)
}

func buildConnectionString(cfg *config.Config) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.DBName,
		cfg.Database.SSLMode,
	)
}

func initDatabaseWithOpener(cfg *config.Config, logger *zap.Logger, opener DBOpener) (*sql.DB, error) {
	connStr := buildConnectionString(cfg)

	db, err := opener.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Database connected successfully",
		zap.String("host", cfg.Database.Host),
		zap.Int("port", cfg.Database.Port),
		zap.String("dbname", cfg.Database.DBName),
	)

	return db, nil
}

func initDatabase(cfg *config.Config, logger *zap.Logger) (*sql.DB, error) {
	return initDatabaseWithOpener(cfg, logger, DefaultDBOpener{})
}
