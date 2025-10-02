package services

import (
	"context"
	"kafka-activity-tracker/domain"
	"kafka-activity-tracker/internal/kafka"
	"strconv"
)

type UserEventService interface {
	SendUserEvent(userID int64, event domain.UserEvent) error
}

type userEventService struct {
	producer kafka.Producer
}

func NewUserEventService(producer kafka.Producer) UserEventService {
	return &userEventService{producer: producer}
}

func (u *userEventService) SendUserEvent(userID int64, event domain.UserEvent) error {
	return u.producer.PublishJSON(context.Background(), domain.EventTopicMap[event.Type], strconv.FormatInt(userID, 10), event)
}
