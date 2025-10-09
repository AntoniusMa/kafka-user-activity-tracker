package main

import (
	"context"
	"fmt"

	"kafka-activity-tracker/config"
	"kafka-activity-tracker/internal/services/user"
	"kafka-activity-tracker/internal/storage/pgsql"

	"go.uber.org/zap"
)

func initLogger(cfg *config.Config) *zap.Logger {
	var logger *zap.Logger
	var err error

	if cfg.App.Environment == "development" {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}

	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	return logger
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	logger := initLogger(cfg)
	defer logger.Sync()

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

	// Initialize database

	db, err := initDatabase(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize database", zap.Error(err))
		return
	}
	defer db.Close()

	// Initialize user repository and service
	userRepo := pgsql.NewUserAdapter(db, logger)
	userService := user.NewUserService(userRepo, logger)

	initKafkaTopics(DefaultDialer{}, cfg.Kafka.Brokers)

	logger.Info("Application started successfully",
		zap.String("app_name", cfg.App.Name),
		zap.String("version", cfg.App.Version),
		zap.String("environment", cfg.App.Environment),
	)

	// Fetch and display a test user
	ctx := context.Background()
	testUser, err := userService.GetUserByID(ctx, "test-001")
	if err != nil {
		logger.Error("Failed to get test user", zap.Error(err))
	} else {
		logger.Info("Successfully fetched user", zap.Any("user", testUser))
	}
}
