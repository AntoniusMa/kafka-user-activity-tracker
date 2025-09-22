package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type MockKafkaWriter struct {
	messages                  []kafka.Message
	expectedWriteMessageError error
	expectedCloseError        error
	closeCalled               bool
}

func (m *MockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.expectedWriteMessageError != nil {
		return m.expectedWriteMessageError
	}

	if m.messages == nil {
		m.messages = []kafka.Message{}
	}

	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *MockKafkaWriter) Close() error {
	m.closeCalled = true
	if m.expectedCloseError != nil {
		return m.expectedCloseError
	}
	return nil
}
func TestNewProducer(t *testing.T) {
	producer := NewProducer([]string{"localhost:8000"})
	require.NotNil(t, producer)
}

func TestPublishJSON(t *testing.T) {
	t.Run("Publish JSON messages", func(t *testing.T) {
		t.Parallel()
		mockWriter := MockKafkaWriter{}
		producer := newProducer(&mockWriter)
		testTopic := "test-topic"
		testKey := "test-key"
		testPayload := map[string]string{"test-map-key-1": "value-1", "test-map-key-2": "value-2"}

		data, err := json.Marshal(testPayload)
		if err != nil {
			t.Fatalf("Failed to marshal test payload: %v", err)
		}
		expectedJSON := string(data)

		err = producer.PublishJSON(context.Background(), testTopic, testKey, testPayload)
		require.NoError(t, err)
		message := mockWriter.messages[0]
		assertSentMessage(t, testTopic, testKey, expectedJSON, message)
	})

	t.Run("Publish multiple JSON messages", func(t *testing.T) {
		t.Parallel()
		mockWriter := MockKafkaWriter{}
		producer := newProducer(&mockWriter)
		testTopic := "test-topic"
		testKey := "test-key"
		testPayload1 := map[string]string{"test-map-key-1": "value-1", "test-map-key-2": "value-2"}
		testPayload2 := []string{"test-list-value-1", "test-list-value-2"}

		data1, err := json.Marshal(testPayload1)
		if err != nil {
			t.Fatalf("Failed to marshal test payload: %v", err)
		}

		data2, err := json.Marshal(testPayload2)
		if err != nil {
			t.Fatalf("Failed to marshal test payload: %v", err)
		}

		err = producer.PublishJSON(context.Background(), testTopic, testKey, testPayload1, testPayload2)
		require.NoError(t, err)

		message1 := mockWriter.messages[0]
		message2 := mockWriter.messages[1]

		assertSentMessage(t, testTopic, testKey, string(data1), message1)
		assertSentMessage(t, testTopic, testKey, string(data2), message2)
	})

	t.Run("Should return err on WriteMessages error", func(t *testing.T) {
		t.Parallel()
		expectError := errors.New("WriteMessage error")
		mockWriter := MockKafkaWriter{expectedWriteMessageError: expectError}
		producer := newProducer((&mockWriter))
		err := producer.PublishJSON(context.Background(), "test-topic", "test-key", nil)
		require.ErrorIs(t, err, expectError)
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()
		mockWriter := MockKafkaWriter{}
		producer := newProducer(&mockWriter)
		err := producer.Close()
		require.NoError(t, err)
		require.True(t, mockWriter.closeCalled)
	})

	t.Run("Error on close", func(t *testing.T) {
		t.Parallel()
		expectedError := errors.New("error on close")
		mockWriter := MockKafkaWriter{expectedCloseError: expectedError}
		producer := newProducer(&mockWriter)
		err := producer.Close()
		require.True(t, mockWriter.closeCalled)
		require.ErrorIs(t, err, expectedError)
	})
}

func assertSentMessage(t testing.TB, topic, key, data string, msg kafka.Message) {
	t.Helper()
	require.Equal(t, data, string(msg.Value))
	require.Equal(t, topic, msg.Topic)
	require.Equal(t, key, string(msg.Key))
}
