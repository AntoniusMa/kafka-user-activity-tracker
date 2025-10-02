package services

import (
	"context"
	"fmt"
	"kafka-activity-tracker/internal/kafka"
	"kafka-activity-tracker/models"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MockSessionRepository struct {
	userEvents map[models.UserEventType][]*models.UserEvent
}

type MockConsumer struct {
	brokers []string
	topic   string
	events  []models.UserEvent
}

func (c *MockConsumer) ConsumeMessages(ctx context.Context, handler kafka.MessageHandler) error {
	for {
		select {
		case <-ctx.Done():
			{
				return ctx.Err()
			}
		default:
			{
				if len(c.events) > 0 {
					event := c.events[0]
					c.events = c.events[1:]
					err := handler(&event)
					if err != nil {
						continue
					}
				}
			}
		}
	}
}

func (c *MockConsumer) Close() error {
	return nil
}

func (msr *MockSessionRepository) TrackUserAction(userAction *models.UserEvent) error {
	if msr.userEvents == nil {
		msr.userEvents = make(map[models.UserEventType][]*models.UserEvent)
	}
	if msr.userEvents[userAction.Type] == nil {
		msr.userEvents[userAction.Type] = []*models.UserEvent{}
	}
	msr.userEvents[userAction.Type] = append(msr.userEvents[userAction.Type], userAction)
	return nil
}

func TestNewEventConsumerService(t *testing.T) {
	t.Parallel()

	repo := MockSessionRepository{}
	capturedConsumerTopics := []string{}
	consumerFactory := func(brokers []string, topic string) kafka.Consumer {
		capturedConsumerTopics = append(capturedConsumerTopics, topic)
		return &MockConsumer{
			brokers: brokers,
			topic:   topic,
		}
	}
	service := NewEventConsumerService(&repo, consumerFactory)

	expectedConsumerTopics := []string{}
	for _, value := range models.EventTopicMap {
		expectedConsumerTopics = append(expectedConsumerTopics, value)
	}
	require.NotNil(t, service)
	require.Equal(t, &repo, service.sessionRepository)
	require.Len(t, capturedConsumerTopics, len(models.EventTopicMap))
	require.ElementsMatch(t, capturedConsumerTopics, expectedConsumerTopics)
}

func TestListenForUserEvents(t *testing.T) {
	testCases := []struct {
		topic         string
		expectedEvent models.UserEvent
	}{}
	eventTime := time.Now()
	userID := "testUser"

	for key, topic := range models.EventTopicMap {
		testCases = append(testCases, struct {
			topic         string
			expectedEvent models.UserEvent
		}{topic: topic, expectedEvent: models.UserEvent{Timestamp: eventTime, Type: key, UserID: userID}})
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("Should track user events on topic: %s", testCase.topic), func(t *testing.T) {
			t.Parallel()
			repo := MockSessionRepository{}
			var consumerFactory func(brokers []string, topic string) kafka.Consumer
			numMessages := map[models.UserEventType]int{}
			numMessages[testCase.expectedEvent.Type] = 1
			consumerFactory = createConsumerFactory(t, userID, numMessages, eventTime)
			service := NewEventConsumerService(&repo, consumerFactory)
			require.NotNil(t, service)
			ctx, close := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer close()

			// this call blocks until context times out
			service.ListenForUserEvents(ctx)

			require.Len(t, repo.userEvents[testCase.expectedEvent.Type], 1)
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			for _, event := range repo.userEvents[testCase.expectedEvent.Type] {
				require.Equal(t, testCase.expectedEvent.Type, event.Type)
				require.Equal(t, testCase.expectedEvent.Timestamp, event.Timestamp)
				require.Equal(t, testCase.expectedEvent.UserID, event.UserID)
			}

		})
	}
}

func createConsumerFactory(t testing.TB, testUserID string, numMessagesForEvent map[models.UserEventType]int, eventTime time.Time) func(brokers []string, topic string) kafka.Consumer {
	t.Helper()
	return func(brokers []string, topic string) kafka.Consumer {
		events := []models.UserEvent{}
		// generate events, a consumer will only ever have events of one type, as each event is mapped to a different topic and each consumer only consumes one topic
		switch topic {
		case models.EventTopicMap[models.LOGIN]:
			{
				for range numMessagesForEvent[models.LOGIN] {
					events = append(events, models.UserEvent{Timestamp: eventTime, Type: models.LOGIN, UserID: testUserID})
				}
			}
		case models.EventTopicMap[models.PAGE_VIEWS]:
			{
				for range numMessagesForEvent[models.PAGE_VIEWS] {
					events = append(events, models.UserEvent{Timestamp: eventTime, Type: models.PAGE_VIEWS, UserID: testUserID})
				}
			}
		default:
			{
				for range numMessagesForEvent[models.USER_ACTION] {
					events = append(events, models.UserEvent{Timestamp: eventTime, Type: models.USER_ACTION, UserID: testUserID})
				}
			}
		}
		return &MockConsumer{
			brokers: brokers,
			topic:   topic,
			events:  events,
		}
	}
}
