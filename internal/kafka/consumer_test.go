package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"kafka-activity-tracker/models"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type MockKafkaReader struct {
	messages                []kafka.Message
	messageIndex            int
	expectedFetchError      error
	expectedCommitError     error
	expectedCloseError      error
	closeCalled             bool
	commitMessagesCallCount int
}

func (m *MockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.expectedFetchError != nil {
		return kafka.Message{}, m.expectedFetchError
	}

	if m.messages == nil || m.messageIndex >= len(m.messages) {
		return kafka.Message{}, errors.New("no more messages")
	}

	message := m.messages[m.messageIndex]
	m.messageIndex++
	return message, nil
}

func (m *MockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.commitMessagesCallCount++
	if m.expectedCommitError != nil {
		return m.expectedCommitError
	}
	return nil
}

func (m *MockKafkaReader) Close() error {
	m.closeCalled = true
	if m.expectedCloseError != nil {
		return m.expectedCloseError
	}
	return nil
}

func TestNewConsumer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	groupID := "test-group"
	topic := "test-topic"

	consumer := NewConsumer(brokers, groupID, topic)
	require.NotNil(t, consumer)
}

func TestConsumeMessages(t *testing.T) {
	t.Run("Consume single message successfully", func(t *testing.T) {
		t.Parallel()
		userEvent := models.UserEvent{
			Timestamp: time.Now(),
			Type:      models.LOGIN,
		}
		eventData, err := json.Marshal(userEvent)
		require.NoError(t, err)

		message := kafka.Message{
			Topic: "test-topic",
			Value: eventData,
		}

		mockReader := &MockKafkaReader{
			messages: []kafka.Message{message},
		}
		consumer := newConsumer(mockReader, "test-topic")

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		handlerCallCount := 0
		handler := func(event *models.UserEvent) error {
			handlerCallCount++
			require.Equal(t, models.LOGIN, event.Type)
			cancel() // Cancel context to exit consume loop
			return nil
		}

		err = consumer.ConsumeMessages(ctx, handler)
		assertMessagingState(t, err, context.Canceled, 1, handlerCallCount, mockReader, 1)
	})

	t.Run("fetch message error", func(t *testing.T) {
		t.Parallel()
		expectedError := errors.New("fetch error")
		mockReader := &MockKafkaReader{
			expectedFetchError: expectedError,
		}
		consumer := newConsumer(mockReader, "test-topic")

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		handlerCallCount := 0
		handler := func(event *models.UserEvent) error {
			handlerCallCount++
			return nil
		}

		err := consumer.ConsumeMessages(ctx, handler)
		assertMessagingState(t, err, context.DeadlineExceeded, 0, handlerCallCount, mockReader, 0)
	})

	t.Run("Handle unmarshal error", func(t *testing.T) {
		t.Parallel()
		message := kafka.Message{
			Topic: "test-topic",
			Value: []byte("invalid json"),
		}

		mockReader := &MockKafkaReader{
			messages: []kafka.Message{message},
		}
		consumer := newConsumer(mockReader, "test-topic")

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		handlerCallCount := 0
		handler := func(event *models.UserEvent) error {
			handlerCallCount++
			return nil
		}

		err := consumer.ConsumeMessages(ctx, handler)
		assertMessagingState(t, err, context.DeadlineExceeded, 0, handlerCallCount, mockReader, 0)
	})

	t.Run("Handle handler error", func(t *testing.T) {
		t.Parallel()
		userEvent := models.UserEvent{
			Timestamp: time.Now(),
			Type:      models.LOGIN,
		}
		eventData, err := json.Marshal(userEvent)
		require.NoError(t, err)

		message := kafka.Message{
			Topic: "test-topic",
			Value: eventData,
		}

		mockReader := &MockKafkaReader{
			messages: []kafka.Message{message},
		}
		consumer := newConsumer(mockReader, "test-topic")

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		handlerCallCount := 0
		handlerError := errors.New("handler error")
		handler := func(event *models.UserEvent) error {
			handlerCallCount++
			cancel() // Cancel context to exit consume loop
			return handlerError
		}

		err = consumer.ConsumeMessages(ctx, handler)
		assertMessagingState(t, err, context.Canceled, 1, handlerCallCount, mockReader, 0)
	})

	t.Run("Handle commit error", func(t *testing.T) {
		t.Parallel()
		userEvent := models.UserEvent{
			Timestamp: time.Now(),
			Type:      models.LOGIN,
		}
		eventData, err := json.Marshal(userEvent)
		require.NoError(t, err)

		message := kafka.Message{
			Topic: "test-topic",
			Value: eventData,
		}

		mockReader := &MockKafkaReader{
			messages:            []kafka.Message{message},
			expectedCommitError: errors.New("commit error"),
		}
		consumer := newConsumer(mockReader, "test-topic")

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		handlerCallCount := 0
		handler := func(event *models.UserEvent) error {
			handlerCallCount++
			cancel() // Cancel context to exit consume loop
			return nil
		}

		err = consumer.ConsumeMessages(ctx, handler)
		assertMessagingState(t, err, context.Canceled, 1, handlerCallCount, mockReader, 1)
	})
}

func TestClose(t *testing.T) {
	t.Run("Close successfully", func(t *testing.T) {
		t.Parallel()
		mockReader := &MockKafkaReader{}
		consumer := newConsumer(mockReader, "test-topic")

		err := consumer.Close()
		require.NoError(t, err)
		require.True(t, mockReader.closeCalled)
	})

	t.Run("Handle close error", func(t *testing.T) {
		t.Parallel()
		expectedError := errors.New("close error")
		mockReader := &MockKafkaReader{
			expectedCloseError: expectedError,
		}
		consumer := newConsumer(mockReader, "test-topic")

		err := consumer.Close()
		require.ErrorIs(t, err, expectedError)
		require.True(t, mockReader.closeCalled)
		require.Contains(t, err.Error(), "failed to close reader for topic test-topic")
	})
}

func TestUnmarshalUserEvent(t *testing.T) {
	t.Run("Unmarshal valid JSON", func(t *testing.T) {
		t.Parallel()
		userEvent := models.UserEvent{
			Timestamp: time.Now(),
			Type:      models.PAGE_VIEWS,
		}
		data, err := json.Marshal(userEvent)
		require.NoError(t, err)

		result, err := unmarshalUserEvent(data)
		require.NoError(t, err)
		require.Equal(t, userEvent.Type, result.Type)
		require.WithinDuration(t, userEvent.Timestamp, result.Timestamp, time.Second)
	})

	t.Run("Handle invalid JSON", func(t *testing.T) {
		t.Parallel()
		invalidData := []byte("invalid json")

		result, err := unmarshalUserEvent(invalidData)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "failed to unmarshal user event")
	})
}

func assertMessagingState(t *testing.T, err error, expectedContextErr error, expectedHandlerCallCount int, handlerCallCount int, mockReader *MockKafkaReader, expectedCommitCallCount int) {
	t.Helper()
	require.ErrorIs(t, err, expectedContextErr)
	require.Equal(t, expectedHandlerCallCount, handlerCallCount)
	require.Equal(t, expectedCommitCallCount, mockReader.commitMessagesCallCount)
}
