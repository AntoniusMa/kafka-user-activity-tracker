package userevents

import (
	"context"
	"errors"
	"kafka-activity-tracker/domain"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockUserEventRepository struct {
	events     []*domain.UserEvent
	trackError error
	getError   error
}

func (m *MockUserEventRepository) TrackUserEvent(ctx context.Context, event *domain.UserEvent) (*domain.UserEvent, error) {
	if m.trackError != nil {
		return nil, m.trackError
	}
	m.events = append(m.events, event)
	return event, nil
}

func (m *MockUserEventRepository) GetEventsForUser(ctx context.Context, userID string) ([]*domain.UserEvent, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	var userEvents []*domain.UserEvent
	for _, event := range m.events {
		if event.UserID == userID {
			userEvents = append(userEvents, event)
		}
	}
	return userEvents, nil
}

func TestNewUserEventService(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := &MockUserEventRepository{}
	service := NewUserEventService(mockRepo, logger)
	require.NotNil(t, service)
}

func TestTrackUserEvent(t *testing.T) {
	logger := zap.NewNop()
	testEvent := &domain.UserEvent{
		ID:        "event-123",
		UserID:    "user-456",
		Timestamp: time.Now(),
		Type:      domain.LOGIN,
	}

	t.Run("track event success", func(t *testing.T) {
		t.Parallel()
		mockRepo := &MockUserEventRepository{}
		service := NewUserEventService(mockRepo, logger)

		result, err := service.TrackUserEvent(context.Background(), testEvent)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, mockRepo.events, 1)
	})

	t.Run("should propagate error on track failure", func(t *testing.T) {
		t.Parallel()
		expectedError := errors.New("track failed")
		mockRepo := &MockUserEventRepository{
			trackError: expectedError,
		}
		service := NewUserEventService(mockRepo, logger)

		_, err := service.TrackUserEvent(context.Background(), testEvent)

		require.ErrorIs(t, err, expectedError)
	})
}

func TestGetEventsForUser(t *testing.T) {
	logger := zap.NewNop()
	userID := "user-123"

	t.Run("successfully get events", func(t *testing.T) {
		t.Parallel()
		mockRepo := &MockUserEventRepository{
			events: []*domain.UserEvent{
				{ID: "1", UserID: userID, Type: domain.LOGIN},
				{ID: "2", UserID: userID, Type: domain.PAGE_VIEWS},
			},
		}
		service := NewUserEventService(mockRepo, logger)

		result, err := service.GetEventsForUser(context.Background(), userID)

		require.NoError(t, err)
		require.Len(t, result, 2)
	})

	t.Run("should propagate error on get failure", func(t *testing.T) {
		t.Parallel()
		expectedError := errors.New("get failed")
		mockRepo := &MockUserEventRepository{
			getError: expectedError,
		}
		service := NewUserEventService(mockRepo, logger)

		_, err := service.GetEventsForUser(context.Background(), userID)

		require.ErrorIs(t, err, expectedError)
	})
}
