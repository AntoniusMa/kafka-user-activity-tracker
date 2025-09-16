package main

import (
	"fmt"

	"kafka-activity-tracker/config"

	"go.uber.org/zap"
)

func initLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	return logger
}

func main() {
	logger := initLogger()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	logger.Info("Configuration loaded successfully")

	logger.Info("Application configuration loaded",
		zap.String("app_name", cfg.App.Name),
		zap.String("version", cfg.App.Version),
		zap.String("environment", cfg.App.Environment),
		zap.String("server_host", cfg.Server.Host),
		zap.Int("server_port", cfg.Server.Port),
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.String("kafka_topic", cfg.Kafka.Topic),
		zap.String("kafka_group_id", cfg.Kafka.GroupID),
		zap.String("log_level", cfg.Logging.Level),
		zap.String("log_format", cfg.Logging.Format),
	)

	logger.Info("Application started successfully",
		zap.String("app_name", cfg.App.Name),
		zap.String("version", cfg.App.Version),
		zap.String("environment", cfg.App.Environment),
	)
}
