package producers

import (
	"context"
	"kafka-activity-tracker/domain"
	"strconv"
)

type UserEventService interface {
	SendUserEvent(userID int64, event domain.UserEvent) error
}

type userEventService struct {
	producer Producer
}

func NewUserEventService(producer Producer) UserEventService {
	return &userEventService{producer: producer}
}

func (u *userEventService) SendUserEvent(userID int64, event domain.UserEvent) error {
	return u.producer.PublishJSON(context.Background(), domain.EventTopicMap[event.Type], strconv.FormatInt(userID, 10), event)
}
