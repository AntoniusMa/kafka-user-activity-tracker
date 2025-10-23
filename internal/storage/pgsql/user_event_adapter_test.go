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

func TestNewUserEventAdapter(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zap.NewNop()

	adapter := NewUserEventAdapter(db, logger)
	require.NotNil(t, adapter)
	require.Implements(t, (*domain.UserEventRepository)(nil), adapter)
}

func TestTrackUserEvent(t *testing.T) {
	logger := zap.NewNop()

	t.Run("successfully track user event", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		testEvent := &domain.UserEvent{
			SessionID: "session-789",
			Type:      domain.LOGIN,
			Timestamp: time.Now(),
		}

		rows := sqlmock.NewRows([]string{"id", "session_id", "type", "timestamp"}).
			AddRow("event-456", testEvent.SessionID, testEvent.Type, testEvent.Timestamp)

		mock.ExpectQuery(`INSERT INTO user_events`).
			WithArgs(testEvent.SessionID, testEvent.Type, testEvent.Timestamp).
			WillReturnRows(rows)

		result, err := adapter.TrackUserEvent(context.Background(), testEvent)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "event-456", result.ID)
		require.Equal(t, testEvent.SessionID, result.SessionID)
		require.Equal(t, testEvent.Type, result.Type)
		require.Equal(t, testEvent.Timestamp, result.Timestamp)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		testEvent := &domain.UserEvent{
			SessionID: "session-789",
			Type:      domain.LOGIN,
			Timestamp: time.Now(),
		}

		mock.ExpectQuery(`INSERT INTO user_events`).
			WithArgs(testEvent.SessionID, testEvent.Type, testEvent.Timestamp).
			WillReturnError(sql.ErrConnDone)

		result, err := adapter.TrackUserEvent(context.Background(), testEvent)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetEventsForUser(t *testing.T) {
	logger := zap.NewNop()
	testUserID := "user-123"
	testTime := time.Now()

	t.Run("should successfully get events for user", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		sessionID1 := "session-123"
		sessionID2 := "session-456"
		rows := sqlmock.NewRows([]string{"id", "session_id", "type", "timestamp"}).
			AddRow("event-1", sessionID1, domain.LOGIN, testTime).
			AddRow("event-2", sessionID2, domain.PAGE_VIEWS, testTime)

		mock.ExpectQuery(`SELECT e\.id, e\.session_id, e\.event_type, e\.timestamp FROM user_events e JOIN user_sessions s ON e\.session_id = s\.session_id WHERE s\.user_id`).
			WithArgs(testUserID).
			WillReturnRows(rows)

		result, err := adapter.GetEventsForUser(context.Background(), testUserID)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result, 2)
		require.Equal(t, "event-1", result[0].ID)
		require.Equal(t, sessionID1, result[0].SessionID)
		require.Equal(t, domain.LOGIN, result[0].Type)
		require.Equal(t, "event-2", result[1].ID)
		require.Equal(t, sessionID2, result[1].SessionID)
		require.Equal(t, domain.PAGE_VIEWS, result[1].Type)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return empty list when no events found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"id", "session_id", "type", "timestamp"})

		mock.ExpectQuery(`SELECT e\.id, e\.session_id, e\.event_type, e\.timestamp FROM user_events e JOIN user_sessions s ON e\.session_id = s\.session_id WHERE s\.user_id`).
			WithArgs(testUserID).
			WillReturnRows(rows)

		result, err := adapter.GetEventsForUser(context.Background(), testUserID)

		require.NoError(t, err)
		require.Len(t, result, 0)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		mock.ExpectQuery(`SELECT e\.id, e\.session_id, e\.event_type, e\.timestamp FROM user_events e JOIN user_sessions s ON e\.session_id = s\.session_id WHERE s\.user_id`).
			WithArgs(testUserID).
			WillReturnError(sql.ErrConnDone)

		result, err := adapter.GetEventsForUser(context.Background(), testUserID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on scan failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"id", "session_id", "type", "timestamp"}).
			AddRow("event-1", "session-123", domain.LOGIN, testTime).
			AddRow("event-2", nil, domain.PAGE_VIEWS, testTime)

		mock.ExpectQuery(`SELECT e\.id, e\.session_id, e\.event_type, e\.timestamp FROM user_events e JOIN user_sessions s ON e\.session_id = s\.session_id WHERE s\.user_id`).
			WithArgs(testUserID).
			WillReturnRows(rows)

		result, err := adapter.GetEventsForUser(context.Background(), testUserID)

		require.Error(t, err)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
