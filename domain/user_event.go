package domain

import (
	"context"
	"time"
)

type UserEventType string

const (
	LOGIN       UserEventType = "LOGIN"
	PAGE_VIEWS  UserEventType = "PAGE-VIEWS"
	USER_ACTION UserEventType = "USER-ACTION"
)

type UserEvent struct {
	ID        string        `json:"id"`
	UserID    string        `json:"userID"`
	Timestamp time.Time     `json:"timestamp"`
	Type      UserEventType `json:"type"`
}

type UserEventRepository interface {
	TrackUserEvent(ctx context.Context, event *UserEvent) (*UserEvent, error)
	GetEventsForUser(ctx context.Context, userID string) ([]*UserEvent, error)
}

var EventTopicMap = map[UserEventType]string{
	LOGIN:       "user-logins",
	PAGE_VIEWS:  "page-views",
	USER_ACTION: "user-actions",
}
