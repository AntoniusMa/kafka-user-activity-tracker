package services

import (
	"context"
	"kafka-activity-tracker/internal/kafka"
	"kafka-activity-tracker/models"
	"strconv"
)

type UserEventService interface {
	SendUserEvent(userID int64, event models.UserEvent) error
}

type userEventService struct {
	producer kafka.Producer
}

func NewUserEventService(producer kafka.Producer) UserEventService {
	return &userEventService{producer: producer}
}

func (u *userEventService) SendUserEvent(userID int64, event models.UserEvent) error {
	return u.producer.PublishJSON(context.Background(), models.EventTopicMap[event.Type], strconv.FormatInt(userID, 10), event)
}
