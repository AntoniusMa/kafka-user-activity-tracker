package pgsql

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"kafka-activity-tracker/domain"

	"go.uber.org/zap"
)

//go:embed queries/session_start.sql
var queryStartSession string

//go:embed queries/session_get_by_id.sql
var queryGetSessionByID string

//go:embed queries/session_get_active_for_user.sql
var queryGetActiveSessionForUser string

//go:embed queries/session_update_status.sql
var queryUpdateSessionStatus string

//go:embed queries/session_delete.sql
var queryDeleteSession string

//go:embed queries/session_logout.sql
var queryLogout string

type SessionAdapter struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewSessionAdapter(db *sql.DB, logger *zap.Logger) domain.UserSessionRepository {
	return &SessionAdapter{
		db:     db,
		logger: logger,
	}
}

func (a *SessionAdapter) StartSession(ctx context.Context, session *domain.UserSession) (*domain.UserSession, error) {
	var createdSession domain.UserSession

	err := a.db.QueryRowContext(ctx, queryStartSession, session.UserID, session.IsActive).
		Scan(&createdSession.SessionID, &createdSession.UserID, &createdSession.IsActive, &createdSession.CreatedAt, &createdSession.UpdatedAt)

	if err != nil {
		a.logger.Error("failed to start session", zap.Error(err), zap.String("user_id", session.UserID))
		return nil, fmt.Errorf("failed to start session: %w", err)
	}

	a.logger.Debug("session started successfully", zap.String("session_id", createdSession.SessionID))
	return &createdSession, nil
}

func (a *SessionAdapter) GetSessionByID(ctx context.Context, sessionID string) (*domain.UserSession, error) {
	var session domain.UserSession
	err := a.db.QueryRowContext(ctx, queryGetSessionByID, sessionID).
		Scan(&session.SessionID, &session.UserID, &session.IsActive, &session.CreatedAt, &session.UpdatedAt)

	if err == sql.ErrNoRows {
		a.logger.Debug("session not found", zap.String("session_id", sessionID))
		return nil, domain.ErrEntityNotFound
	}

	if err != nil {
		a.logger.Error("failed to get session", zap.Error(err), zap.String("session_id", sessionID))
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	return &session, nil
}

func (a *SessionAdapter) GetActiveSessionForUser(ctx context.Context, userID string) (*domain.UserSession, error) {
	var session domain.UserSession
	err := a.db.QueryRowContext(ctx, queryGetActiveSessionForUser, userID).
		Scan(&session.SessionID, &session.UserID, &session.IsActive, &session.CreatedAt, &session.UpdatedAt)

	if err == sql.ErrNoRows {
		a.logger.Debug("no active session found for user", zap.String("user_id", userID))
		return nil, domain.ErrEntityNotFound
	}

	if err != nil {
		a.logger.Error("failed to get active session", zap.Error(err), zap.String("user_id", userID))
		return nil, fmt.Errorf("failed to get active session: %w", err)
	}

	return &session, nil
}

func (a *SessionAdapter) UpdateSessionStatus(ctx context.Context, sessionID string, isActive bool) error {
	result, err := a.db.ExecContext(ctx, queryUpdateSessionStatus, sessionID, isActive)
	if err != nil {
		a.logger.Error("failed to update session status", zap.Error(err), zap.String("session_id", sessionID))
		return fmt.Errorf("failed to update session status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		a.logger.Error("failed to get rows affected", zap.Error(err))
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		a.logger.Debug("session not found for update", zap.String("session_id", sessionID))
		return domain.ErrEntityNotFound
	}

	a.logger.Debug("session status updated successfully", zap.String("session_id", sessionID), zap.Bool("is_active", isActive))
	return nil
}

func (a *SessionAdapter) DeleteSession(ctx context.Context, sessionID string) error {
	result, err := a.db.ExecContext(ctx, queryDeleteSession, sessionID)
	if err != nil {
		a.logger.Error("failed to delete session", zap.Error(err), zap.String("session_id", sessionID))
		return fmt.Errorf("failed to delete session: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		a.logger.Error("failed to get rows affected", zap.Error(err))
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		a.logger.Debug("session not found for deletion", zap.String("session_id", sessionID))
		return domain.ErrEntityNotFound
	}

	a.logger.Debug("session deleted successfully", zap.String("session_id", sessionID))
	return nil
}

func (a *SessionAdapter) Logout(ctx context.Context, userID string) error {
	result, err := a.db.ExecContext(ctx, queryLogout, userID)
	if err != nil {
		a.logger.Error("failed to logout user", zap.Error(err), zap.String("user_id", userID))
		return fmt.Errorf("failed to logout user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		a.logger.Error("failed to get rows affected", zap.Error(err))
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		a.logger.Debug("no active session found for logout", zap.String("user_id", userID))
		return domain.ErrEntityNotFound
	}

	a.logger.Info("user logged out successfully", zap.String("user_id", userID))
	return nil
}
