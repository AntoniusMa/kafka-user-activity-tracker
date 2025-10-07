package user

import (
	"context"
	"fmt"
	"kafka-activity-tracker/domain"

	"go.uber.org/zap"
)

type UserService interface {
	CreateUser(ctx context.Context, user *domain.User) (*domain.User, error)
	GetUserByID(ctx context.Context, id string) (*domain.User, error)
	DeleteUserByID(ctx context.Context, id string) error
}

type userService struct {
	logger   *zap.Logger
	userRepo domain.UserRepository
}

func NewUserService(userRepository domain.UserRepository, logger *zap.Logger) UserService {
	return &userService{userRepo: userRepository, logger: logger}
}

func (u *userService) CreateUser(ctx context.Context, user *domain.User) (*domain.User, error) {
	u.logger.Debug(fmt.Sprintf("creating new user: %s", user.GetFullName()))

	return u.userRepo.Create(ctx, user)
}

func (u *userService) GetUserByID(ctx context.Context, id string) (*domain.User, error) {
	return u.userRepo.GetByID(ctx, id)
}

func (u *userService) DeleteUserByID(ctx context.Context, id string) error {
	return u.userRepo.DeleteByID(ctx, id)
}
