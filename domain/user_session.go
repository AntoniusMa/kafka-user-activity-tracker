package domain

import (
	"context"
	"time"
)

type UserSession struct {
	SessionID string
	UserID    string
	IsActive  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type UserSessionRepository interface {
	StartSession(ctx context.Context, session *UserSession) (*UserSession, error)
	GetSessionByID(ctx context.Context, sessionID string) (*UserSession, error)
	GetActiveSessionForUser(ctx context.Context, userID string) (*UserSession, error)
	UpdateSessionStatus(ctx context.Context, sessionID string, isActive bool) error
	DeleteSession(ctx context.Context, sessionID string) error
	Logout(ctx context.Context, userID string) error
}
