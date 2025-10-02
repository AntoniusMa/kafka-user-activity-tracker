package models

import "time"

type UserEventType string

const (
	LOGIN       UserEventType = "LOGIN"
	PAGE_VIEWS  UserEventType = "PAGE-VIEWS"
	USER_ACTION UserEventType = "USER-ACTION"
)

type UserEvent struct {
	UserID    string        `json:"userID"`
	Timestamp time.Time     `json:"timestamp"`
	Type      UserEventType `json:"type"`
}

var EventTopicMap = map[UserEventType]string{
	LOGIN:       "user-logins",
	PAGE_VIEWS:  "page-views",
	USER_ACTION: "user-actions",
}
