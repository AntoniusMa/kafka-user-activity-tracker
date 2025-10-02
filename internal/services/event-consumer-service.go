package services

import (
	"context"
	"kafka-activity-tracker/domain"
	"kafka-activity-tracker/internal/kafka"
	"sync"
)

type SessionRepository interface {
	TrackUserAction(userAction *domain.UserEvent) error
}

type EventConsumerService struct {
	consumers         []kafka.Consumer
	sessionRepository SessionRepository
}

func NewEventConsumerService(repo SessionRepository, consumerFactory func(brokers []string, topic string) kafka.Consumer) EventConsumerService {
	consumers := []kafka.Consumer{}
	for _, topic := range domain.EventTopicMap {
		consumers = append(consumers, consumerFactory([]string{"localhost:8000"}, topic))
	}
	return EventConsumerService{
		sessionRepository: repo,
		consumers:         consumers,
	}
}

func (e *EventConsumerService) ListenForUserEvents(ctx context.Context) {
	var wg sync.WaitGroup

	for _, consumer := range e.consumers {
		wg.Go(func() {
			consumer.ConsumeMessages(ctx, func(event *domain.UserEvent) error {
				err := e.sessionRepository.TrackUserAction(event)
				return err
			})
		})
	}

	wg.Wait()
}
