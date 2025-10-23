package pgsql

import (
	"context"
	"database/sql"
	"kafka-activity-tracker/domain"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewSessionAdapter(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zap.NewNop()

	adapter := NewSessionAdapter(db, logger)
	require.NotNil(t, adapter)
	require.Implements(t, (*domain.UserSessionRepository)(nil), adapter)
}

func TestStartSession(t *testing.T) {
	logger := zap.NewNop()

	testSession := &domain.UserSession{
		UserID:   "user-123",
		IsActive: true,
	}

	t.Run("successfully start session", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		now := time.Now()
		testSession.SessionID = "session-456"
		rows := sqlmock.NewRows([]string{"session_id", "user_id", "is_active", "created_at", "updated_at"}).
			AddRow(testSession.SessionID, testSession.UserID, testSession.IsActive, now, now)

		mock.ExpectQuery(`INSERT INTO user_sessions`).
			WithArgs(testSession.UserID, testSession.IsActive).
			WillReturnRows(rows)

		result, err := adapter.StartSession(context.Background(), testSession)

		testSession.CreatedAt = now
		testSession.UpdatedAt = now

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, testSession, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectQuery(`INSERT INTO user_sessions`).
			WithArgs(testSession.UserID, testSession.IsActive).
			WillReturnError(sql.ErrConnDone)

		result, err := adapter.StartSession(context.Background(), testSession)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetSessionByID(t *testing.T) {
	logger := zap.NewNop()
	now := time.Now()

	testSession := &domain.UserSession{
		SessionID: "session-123",
		UserID:    "user-123",
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("should successfully get session", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"session_id", "user_id", "is_active", "created_at", "updated_at"}).
			AddRow(testSession.SessionID, testSession.UserID, testSession.IsActive, testSession.CreatedAt, testSession.UpdatedAt)

		mock.ExpectQuery(`SELECT .* FROM user_sessions WHERE session_id = \$1`).
			WithArgs(testSession.SessionID).
			WillReturnRows(rows)

		result, err := adapter.GetSessionByID(context.Background(), testSession.SessionID)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, testSession, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when session not found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectQuery(`SELECT .* FROM user_sessions WHERE session_id = \$1`).
			WithArgs(testSession.SessionID).
			WillReturnError(sql.ErrNoRows)

		result, err := adapter.GetSessionByID(context.Background(), testSession.SessionID)

		require.ErrorIs(t, err, domain.ErrEntityNotFound)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectQuery(`SELECT .* FROM user_sessions WHERE session_id = \$1`).
			WithArgs(testSession.SessionID).
			WillReturnError(sql.ErrConnDone)

		result, err := adapter.GetSessionByID(context.Background(), testSession.SessionID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetActiveSessionForUser(t *testing.T) {
	logger := zap.NewNop()
	now := time.Now()

	testSession := &domain.UserSession{
		SessionID: "session-123",
		UserID:    "user-123",
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("should successfully get active session for user", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"session_id", "user_id", "is_active", "created_at", "updated_at"}).
			AddRow(testSession.SessionID, testSession.UserID, testSession.IsActive, testSession.CreatedAt, testSession.UpdatedAt)

		mock.ExpectQuery(`SELECT .* FROM user_sessions WHERE user_id = \$1 AND is_active = TRUE`).
			WithArgs(testSession.UserID).
			WillReturnRows(rows)

		result, err := adapter.GetActiveSessionForUser(context.Background(), testSession.UserID)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, testSession, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when no active session found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectQuery(`SELECT .* FROM user_sessions WHERE user_id = \$1 AND is_active = TRUE`).
			WithArgs(testSession.UserID).
			WillReturnError(sql.ErrNoRows)

		result, err := adapter.GetActiveSessionForUser(context.Background(), testSession.UserID)

		require.ErrorIs(t, err, domain.ErrEntityNotFound)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectQuery(`SELECT .* FROM user_sessions WHERE user_id = \$1 AND is_active = TRUE`).
			WithArgs(testSession.UserID).
			WillReturnError(sql.ErrConnDone)

		result, err := adapter.GetActiveSessionForUser(context.Background(), testSession.UserID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestUpdateSessionStatus(t *testing.T) {
	logger := zap.NewNop()
	testSessionID := "session-123"

	t.Run("should successfully update session status", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`UPDATE user_sessions`).
			WithArgs(testSessionID, false).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = adapter.UpdateSessionStatus(context.Background(), testSessionID, false)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when session not found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`UPDATE user_sessions`).
			WithArgs(testSessionID, false).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = adapter.UpdateSessionStatus(context.Background(), testSessionID, false)

		require.ErrorIs(t, err, domain.ErrEntityNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`UPDATE user_sessions`).
			WithArgs(testSessionID, false).
			WillReturnError(sql.ErrConnDone)

		err = adapter.UpdateSessionStatus(context.Background(), testSessionID, false)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestDeleteSession(t *testing.T) {
	logger := zap.NewNop()
	testSessionID := "session-123"

	t.Run("should successfully delete session", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`DELETE FROM user_sessions WHERE session_id = \$1`).
			WithArgs(testSessionID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = adapter.DeleteSession(context.Background(), testSessionID)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when session not found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`DELETE FROM user_sessions WHERE session_id = \$1`).
			WithArgs(testSessionID).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = adapter.DeleteSession(context.Background(), testSessionID)

		require.ErrorIs(t, err, domain.ErrEntityNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`DELETE FROM user_sessions WHERE session_id = \$1`).
			WithArgs(testSessionID).
			WillReturnError(sql.ErrConnDone)

		err = adapter.DeleteSession(context.Background(), testSessionID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestLogout(t *testing.T) {
	logger := zap.NewNop()
	testUserID := "user-123"

	t.Run("should successfully logout user", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`UPDATE user_sessions`).
			WithArgs(testUserID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = adapter.Logout(context.Background(), testUserID)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when no active session found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`UPDATE user_sessions`).
			WithArgs(testUserID).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = adapter.Logout(context.Background(), testUserID)

		require.ErrorIs(t, err, domain.ErrEntityNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewSessionAdapter(db, logger)

		mock.ExpectExec(`UPDATE user_sessions`).
			WithArgs(testUserID).
			WillReturnError(sql.ErrConnDone)

		err = adapter.Logout(context.Background(), testUserID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
