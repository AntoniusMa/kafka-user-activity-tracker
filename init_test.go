package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"kafka-activity-tracker/config"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockKafkaConn struct {
	topics        []kafka.TopicConfig
	expectedError error
	closeCalled   bool
}

func (m *MockKafkaConn) CreateTopics(topics ...kafka.TopicConfig) error {
	if m.expectedError != nil {
		return m.expectedError
	}
	m.topics = append(m.topics, topics...)
	return nil
}

func (m *MockKafkaConn) Close() error {
	m.closeCalled = true
	return nil
}

type MockDialer struct {
	conn             MockKafkaConn
	expectedError    error
	topicCreateError error
}

func (m *MockDialer) DialContext(ctx context.Context, network, address string) (KafkaConn, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	m.conn = MockKafkaConn{}
	if m.topicCreateError != nil {
		m.conn.expectedError = m.topicCreateError
	}
	return &m.conn, nil
}

func TestInitKafkaTopics(t *testing.T) {

	t.Run("Should create topics", func(t *testing.T) {
		t.Parallel()
		mockDialer := MockDialer{}
		initKafkaTopics(&mockDialer, []string{"localhost:8000"})

		require.NotNil(t, mockDialer.conn)
		require.Equal(t, basicTopics, mockDialer.conn.topics)

		require.True(t, mockDialer.conn.closeCalled)
	})

	t.Run("Should return dial error", func(t *testing.T) {
		t.Parallel()
		mockDialer := MockDialer{}
		mockDialer.expectedError = errors.New("dial failed")

		err := initKafkaTopics(&mockDialer, []string{"localhost:8000"})
		require.ErrorIs(t, err, mockDialer.expectedError)
	})

	t.Run("Should return create topics error", func(t *testing.T) {
		t.Parallel()
		mockDialer := MockDialer{}
		mockDialer.topicCreateError = errors.New("topic create error")

		err := initKafkaTopics(&mockDialer, []string{"localhost:8000"})
		require.ErrorIs(t, err, mockDialer.topicCreateError)
	})

}

func TestBuildConnectionString(t *testing.T) {
	t.Run("should build correct connection string", func(t *testing.T) {
		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				DBName:   "testdb",
				SSLMode:  "disable",
			},
		}

		expected := "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"
		actual := buildConnectionString(cfg)

		require.Equal(t, expected, actual)
	})

	t.Run("should handle different ports and ssl modes", func(t *testing.T) {
		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Host:     "db.example.com",
				Port:     5433,
				User:     "admin",
				Password: "secret123",
				DBName:   "production",
				SSLMode:  "require",
			},
		}

		expected := "host=db.example.com port=5433 user=admin password=secret123 dbname=production sslmode=require"
		actual := buildConnectionString(cfg)

		require.Equal(t, expected, actual)
	})
}

type MockDBOpener struct {
	db            *sql.DB
	expectedError error
	openCalled    bool
}

func (m *MockDBOpener) Open(driverName, dataSourceName string) (*sql.DB, error) {
	m.openCalled = true
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return m.db, nil
}

func TestInitDatabase(t *testing.T) {
	logger := zap.NewNop()

	t.Run("should successfully connect to database", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				DBName:   "testdb",
				SSLMode:  "disable",
			},
		}

		mockOpener := &MockDBOpener{db: db}
		result, err := initDatabaseWithOpener(cfg, logger, mockOpener)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, mockOpener.openCalled)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when sql.Open fails", func(t *testing.T) {
		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				DBName:   "testdb",
				SSLMode:  "disable",
			},
		}

		expectedErr := errors.New("failed to open connection")
		mockOpener := &MockDBOpener{expectedError: expectedErr}

		_, err := initDatabaseWithOpener(cfg, logger, mockOpener)

		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to open database")
		require.True(t, mockOpener.openCalled)
	})

	t.Run("should return error on ping failure", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		pingErr := errors.New("connection refused")
		mock.ExpectPing().WillReturnError(pingErr)

		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				DBName:   "testdb",
				SSLMode:  "disable",
			},
		}

		mockOpener := &MockDBOpener{db: db}
		_, err = initDatabaseWithOpener(cfg, logger, mockOpener)

		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to ping database")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should use correct connection string", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Host:     "db.example.com",
				Port:     5433,
				User:     "admin",
				Password: "secret123",
				DBName:   "production",
				SSLMode:  "require",
			},
		}

		mockOpener := &MockDBOpener{db: db}
		result, err := initDatabaseWithOpener(cfg, logger, mockOpener)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
