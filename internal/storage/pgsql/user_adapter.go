package pgsql

import (
	"context"
	"database/sql"
	"fmt"
	"kafka-activity-tracker/domain"

	"go.uber.org/zap"
)

type UserAdapter struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewUserAdapter(db *sql.DB, logger *zap.Logger) domain.UserRepository {
	return &UserAdapter{
		db:     db,
		logger: logger,
	}
}

func (r *UserAdapter) Create(ctx context.Context, user *domain.User) (*domain.User, error) {
	query := `
		INSERT INTO users (user_id, first_name, last_name)
		VALUES ($1, $2, $3)
		RETURNING user_id, first_name, last_name
	`

	var createdUser domain.User

	err := r.db.QueryRowContext(ctx, query, user.UserID, user.FirstName, user.LastName).
		Scan(&createdUser.UserID, &createdUser.FirstName, &createdUser.LastName)

	if err != nil {
		r.logger.Error("failed to create user", zap.Error(err), zap.String("user_id", user.UserID))
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	r.logger.Debug("user created successfully", zap.String("user_id", createdUser.UserID))
	return &createdUser, nil
}

func (r *UserAdapter) GetByID(ctx context.Context, id string) (*domain.User, error) {
	query := `
		SELECT user_id, first_name, last_name
		FROM users
		WHERE user_id = $1
	`

	var user domain.User
	err := r.db.QueryRowContext(ctx, query, id).
		Scan(&user.UserID, &user.FirstName, &user.LastName)

	if err == sql.ErrNoRows {
		r.logger.Debug("user not found", zap.String("user_id", id))
		return nil, domain.ErrEntityNotFound
	}

	if err != nil {
		r.logger.Error("failed to get user", zap.Error(err), zap.String("user_id", id))
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return &user, nil
}

func (r *UserAdapter) DeleteByID(ctx context.Context, id string) error {

	query := `
		DELETE FROM users
		WHERE user_id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		r.logger.Error("failed to delete user", zap.Error(err), zap.String("user_id", id))
		return fmt.Errorf("failed to delete user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		r.logger.Error("failed to get rows affected", zap.Error(err))
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		r.logger.Debug("user not found for deletion", zap.String("user_id", id))
		return domain.ErrEntityNotFound
	}

	r.logger.Debug("user deleted successfully", zap.String("user_id", id))
	return nil
}
