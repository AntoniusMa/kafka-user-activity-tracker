package pgsql

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"kafka-activity-tracker/domain"

	"go.uber.org/zap"
)

//go:embed queries/user_event_create.sql
var queryCreateUserEvent string

//go:embed queries/user_event_get_by_user_id.sql
var queryGetUserEventsByUserID string

type UserEventAdapter struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewUserEventAdapter(db *sql.DB, logger *zap.Logger) domain.UserEventRepository {
	return &UserEventAdapter{
		db:     db,
		logger: logger,
	}
}

func (a *UserEventAdapter) TrackUserEvent(ctx context.Context, event *domain.UserEvent) (*domain.UserEvent, error) {
	var createdEvent domain.UserEvent

	err := a.db.QueryRowContext(ctx, queryCreateUserEvent, event.UserID, event.Type, event.Timestamp).
		Scan(&createdEvent.ID, &createdEvent.UserID, &createdEvent.Type, &createdEvent.Timestamp)

	if err != nil {
		a.logger.Error("failed to create user event", zap.Error(err), zap.String("user_id", event.UserID))
		return nil, fmt.Errorf("failed to create user event: %w", err)
	}

	a.logger.Debug("user event created successfully", zap.String("event_id", createdEvent.ID))
	return &createdEvent, nil
}

func (a *UserEventAdapter) GetEventsForUser(ctx context.Context, userID string) ([]*domain.UserEvent, error) {
	rows, err := a.db.QueryContext(ctx, queryGetUserEventsByUserID, userID)
	if err != nil {
		a.logger.Error("failed to get user events", zap.Error(err), zap.String("user_id", userID))
		return nil, fmt.Errorf("failed to get user events: %w", err)
	}
	defer rows.Close()

	var events []*domain.UserEvent
	for rows.Next() {
		var event domain.UserEvent
		if err := rows.Scan(&event.ID, &event.UserID, &event.Type, &event.Timestamp); err != nil {
			a.logger.Error("failed to scan user event", zap.Error(err))
			return nil, fmt.Errorf("failed to scan user event: %w", err)
		}
		events = append(events, &event)
	}

	if err := rows.Err(); err != nil {
		a.logger.Error("error iterating user events", zap.Error(err))
		return nil, fmt.Errorf("error iterating user events: %w", err)
	}

	return events, nil
}
