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

	testEvent := &domain.UserEvent{
		UserID:    "user-123",
		Type:      domain.LOGIN,
		Timestamp: time.Now(),
	}

	t.Run("successfully track user event", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"id", "user_id", "type", "timestamp"}).
			AddRow("event-456", testEvent.UserID, testEvent.Type, testEvent.Timestamp)

		mock.ExpectQuery(`INSERT INTO user_events`).
			WithArgs(testEvent.UserID, testEvent.Type, testEvent.Timestamp).
			WillReturnRows(rows)

		result, err := adapter.TrackUserEvent(context.Background(), testEvent)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "event-456", result.ID)
		require.Equal(t, testEvent.UserID, result.UserID)
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

		mock.ExpectQuery(`INSERT INTO user_events`).
			WithArgs(testEvent.UserID, testEvent.Type, testEvent.Timestamp).
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

		rows := sqlmock.NewRows([]string{"id", "user_id", "type", "timestamp"}).
			AddRow("event-1", testUserID, domain.LOGIN, testTime).
			AddRow("event-2", testUserID, domain.PAGE_VIEWS, testTime)

		mock.ExpectQuery(`SELECT .* FROM user_events WHERE user_id = \$1`).
			WithArgs(testUserID).
			WillReturnRows(rows)

		result, err := adapter.GetEventsForUser(context.Background(), testUserID)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result, 2)
		require.Equal(t, "event-1", result[0].ID)
		require.Equal(t, testUserID, result[0].UserID)
		require.Equal(t, domain.LOGIN, result[0].Type)
		require.Equal(t, "event-2", result[1].ID)
		require.Equal(t, domain.PAGE_VIEWS, result[1].Type)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return empty list when no events found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserEventAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"id", "user_id", "type", "timestamp"})

		mock.ExpectQuery(`SELECT .* FROM user_events WHERE user_id = \$1`).
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

		mock.ExpectQuery(`SELECT .* FROM user_events WHERE user_id = \$1`).
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

		rows := sqlmock.NewRows([]string{"id", "user_id", "type", "timestamp"}).
			AddRow("event-1", testUserID, domain.LOGIN, testTime).
			AddRow("event-2", nil, domain.PAGE_VIEWS, testTime)

		mock.ExpectQuery(`SELECT .* FROM user_events WHERE user_id = \$1`).
			WithArgs(testUserID).
			WillReturnRows(rows)

		result, err := adapter.GetEventsForUser(context.Background(), testUserID)

		require.Error(t, err)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
