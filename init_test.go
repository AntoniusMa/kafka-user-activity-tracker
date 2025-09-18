package main

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
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
