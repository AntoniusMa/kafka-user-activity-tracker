package userevents

import (
	"context"
	"kafka-activity-tracker/domain"

	"go.uber.org/zap"
)

type UserEventService interface {
	TrackUserEvent(ctx context.Context, event *domain.UserEvent) (*domain.UserEvent, error)
	GetEventsForUser(ctx context.Context, userID string) ([]*domain.UserEvent, error)
}

type userEventService struct {
	logger        *zap.Logger
	userEventRepo domain.UserEventRepository
}

func NewUserEventService(userEventRepository domain.UserEventRepository, logger *zap.Logger) UserEventService {
	return &userEventService{
		userEventRepo: userEventRepository,
		logger:        logger,
	}
}

func (s *userEventService) TrackUserEvent(ctx context.Context, event *domain.UserEvent) (*domain.UserEvent, error) {
	return s.userEventRepo.TrackUserEvent(ctx, &domain.UserEvent{})
}

func (s *userEventService) GetEventsForUser(ctx context.Context, userID string) ([]*domain.UserEvent, error) {
	return s.userEventRepo.GetEventsForUser(ctx, userID)
}
