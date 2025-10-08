package pgsql

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"kafka-activity-tracker/domain"

	"go.uber.org/zap"
)

//go:embed queries/user_create.sql
var queryCreateUser string

//go:embed queries/user_get.sql
var queryGetUser string

//go:embed queries/user_delete.sql
var queryDeleteUser string

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
	var createdUser domain.User

	err := r.db.QueryRowContext(ctx, queryCreateUser, user.UserID, user.FirstName, user.LastName).
		Scan(&createdUser.UserID, &createdUser.FirstName, &createdUser.LastName)

	if err != nil {
		r.logger.Error("failed to create user", zap.Error(err), zap.String("user_id", user.UserID))
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	r.logger.Debug("user created successfully", zap.String("user_id", createdUser.UserID))
	return &createdUser, nil
}

func (r *UserAdapter) GetByID(ctx context.Context, id string) (*domain.User, error) {
	var user domain.User
	err := r.db.QueryRowContext(ctx, queryGetUser, id).
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
	result, err := r.db.ExecContext(ctx, queryDeleteUser, id)
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
