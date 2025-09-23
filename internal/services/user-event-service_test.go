package services

import (
	"context"
	"errors"
	"fmt"
	"kafka-activity-tracker/models"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MockPublishedEvent struct {
	Topic string
	Key   string
	msg   models.UserEvent
}

type MockKafkaProducer struct {
	publishedMessages []MockPublishedEvent
	publishError      error
}

func (m *MockKafkaProducer) PublishJSON(ctx context.Context, topic, key string, msgs ...any) error {
	if m.publishError != nil {
		return m.publishError
	}

	if m.publishedMessages == nil {
		m.publishedMessages = []MockPublishedEvent{}
	}

	for _, msg := range msgs {
		castMessage, ok := msg.(models.UserEvent)
		if !ok {
			return errors.New("msg was not a user event")
		}
		m.publishedMessages = append(m.publishedMessages, MockPublishedEvent{Topic: topic, Key: key, msg: castMessage})
	}
	return nil
}

func (m *MockKafkaProducer) Close() error {
	return nil
}

func TestNewUserEventService(t *testing.T) {
	producer := MockKafkaProducer{}
	service := NewUserEventService(&producer)
	require.NotNil(t, service)
}

func TestSendUserEvent(t *testing.T) {
	testCases := []struct {
		Event       models.UserEvent
		TargetTopic string
	}{
		{
			Event:       models.UserEvent{Timestamp: time.Now(), Type: models.LOGIN},
			TargetTopic: models.EventTopicMap[models.LOGIN],
		},
		{
			Event:       models.UserEvent{Timestamp: time.Now(), Type: models.PAGE_VIEWS},
			TargetTopic: models.EventTopicMap[models.PAGE_VIEWS],
		},
		{
			Event:       models.UserEvent{Timestamp: time.Now(), Type: models.USER_ACTION},
			TargetTopic: models.EventTopicMap[models.USER_ACTION],
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("Send %s events to topic: %s", testCase.Event.Type, testCase.TargetTopic), func(t *testing.T) {
			t.Parallel()
			producer := MockKafkaProducer{}
			service := NewUserEventService(&producer)
			testUserID := int64(1)
			err := service.SendUserEvent(testUserID, testCase.Event)
			require.NotNil(t, producer.publishedMessages)
			require.NoError(t, err)
			sentEvent := producer.publishedMessages[0]
			require.Equal(t, testCase.Event, sentEvent.msg)
			require.Equal(t, testCase.TargetTopic, sentEvent.Topic)
			require.Equal(t, strconv.FormatInt(testUserID, 10), sentEvent.Key)
		})
	}

	t.Run("Returns PublishJSON error", func(t *testing.T) {
		t.Parallel()
		producer := MockKafkaProducer{}
		expectedError := errors.New("publish error")
		producer.publishError = expectedError
		service := NewUserEventService(&producer)
		err := service.SendUserEvent(1, models.UserEvent{})
		require.ErrorIs(t, err, expectedError)
	})
}
