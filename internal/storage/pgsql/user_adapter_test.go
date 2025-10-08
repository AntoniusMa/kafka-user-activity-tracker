package pgsql

import (
	"context"
	"database/sql"
	"kafka-activity-tracker/domain"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewUserAdapter(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zap.NewNop()

	adapter := NewUserAdapter(db, logger)
	require.NotNil(t, adapter)
	require.Implements(t, (*domain.UserRepository)(nil), adapter)
}

func TestCreate(t *testing.T) {
	logger := zap.NewNop()

	testUser := &domain.User{
		UserID:    "test-123",
		FirstName: "John",
		LastName:  "Doe",
	}

	t.Run("successfully create user", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"user_id", "first_name", "last_name"}).
			AddRow(testUser.UserID, testUser.FirstName, testUser.LastName)

		mock.ExpectQuery(`INSERT INTO users`).
			WithArgs(testUser.UserID, testUser.FirstName, testUser.LastName).
			WillReturnRows(rows)

		result, err := adapter.Create(context.Background(), testUser)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, testUser.UserID, result.UserID)
		require.Equal(t, testUser.FirstName, result.FirstName)
		require.Equal(t, testUser.LastName, result.LastName)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		mock.ExpectQuery(`INSERT INTO users`).
			WithArgs(testUser.UserID, testUser.FirstName, testUser.LastName).
			WillReturnError(sql.ErrConnDone)

		result, err := adapter.Create(context.Background(), testUser)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetByID(t *testing.T) {
	logger := zap.NewNop()

	testUser := &domain.User{
		UserID:    "test-123",
		FirstName: "John",
		LastName:  "Doe",
	}

	t.Run("should successfully get user", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		rows := sqlmock.NewRows([]string{"user_id", "first_name", "last_name"}).
			AddRow(testUser.UserID, testUser.FirstName, testUser.LastName)

		mock.ExpectQuery(`SELECT .* FROM users WHERE user_id = \$1`).WithArgs(testUser.UserID).WillReturnRows(rows)

		result, err := adapter.GetByID(context.Background(), testUser.UserID)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, testUser.UserID, result.UserID)
		require.Equal(t, testUser.FirstName, result.FirstName)
		require.Equal(t, testUser.LastName, result.LastName)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		mock.ExpectQuery(`SELECT .* FROM users WHERE user_id = \$1`).WithArgs(testUser.UserID).WillReturnError(sql.ErrConnDone)

		result, err := adapter.GetByID(context.Background(), testUser.UserID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestDeleteByID(t *testing.T) {
	logger := zap.NewNop()
	testUserID := "test-123"

	t.Run("should successfully delete user", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		mock.ExpectExec(`DELETE FROM users WHERE user_id = \$1`).
			WithArgs(testUserID).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = adapter.DeleteByID(context.Background(), testUserID)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error when user not found", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		mock.ExpectExec(`DELETE FROM users WHERE user_id = \$1`).
			WithArgs(testUserID).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = adapter.DeleteByID(context.Background(), testUserID)

		require.ErrorIs(t, err, domain.ErrEntityNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("should return error on database failure", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		adapter := NewUserAdapter(db, logger)

		mock.ExpectExec(`DELETE FROM users WHERE user_id = \$1`).
			WithArgs(testUserID).
			WillReturnError(sql.ErrConnDone)

		err = adapter.DeleteByID(context.Background(), testUserID)

		require.ErrorIs(t, err, sql.ErrConnDone)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
