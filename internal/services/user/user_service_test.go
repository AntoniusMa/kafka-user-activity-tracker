package user

import (
	"context"
	"errors"
	"kafka-activity-tracker/domain"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockUserRepository struct {
	users        []*domain.User
	createError  error
	getUserError error
	deleteError  error
}

func (u *MockUserRepository) Create(ctx context.Context, user *domain.User) (*domain.User, error) {
	if err := u.createError; err != nil {
		return nil, err
	}

	u.users = append(u.users, user)
	return user, nil
}

func (u *MockUserRepository) GetByID(ctx context.Context, id string) (*domain.User, error) {
	if err := u.getUserError; err != nil {
		return nil, err
	}

	for _, user := range u.users {
		if user.UserID == id {
			return user, nil
		}
	}

	return nil, errors.New("user not found")
}

func (u *MockUserRepository) DeleteByID(ctx context.Context, id string) error {
	if err := u.deleteError; err != nil {
		return err
	}
	for i, user := range u.users {
		if user.UserID == id {
			u.users = append(u.users[:i], u.users[i+1:]...)
			return nil
		}
	}
	return errors.New(("user not found"))
}

func TestNewUserService(t *testing.T) {
	logger := zap.NewNop()
	mockRepository := MockUserRepository{
		users: []*domain.User{},
	}
	service := NewUserService(&mockRepository, logger)
	require.NotNil(t, service)
}

func TestCreateUser(t *testing.T) {
	logger := zap.NewNop()
	testUser := domain.User{
		UserID:    "test-id",
		FirstName: "Billiam",
		LastName:  "Gates",
	}
	t.Run("create user success", func(t *testing.T) {
		t.Parallel()
		mockRepository := MockUserRepository{
			users: []*domain.User{},
		}

		service := NewUserService(&mockRepository, logger)
		user, err := service.CreateUser(context.Background(), &testUser)
		require.Nil(t, err)
		require.Equal(t, testUser, *user)
		require.Equal(t, mockRepository.users[0], user)
	})

	t.Run("create fail should propagate error", func(t *testing.T) {
		t.Parallel()

		expectedError := errors.New("failed to create user")
		mockRepository := MockUserRepository{
			users:       []*domain.User{},
			createError: expectedError,
		}

		service := NewUserService(&mockRepository, logger)
		_, err := service.CreateUser(context.Background(), &testUser)
		require.ErrorIs(t, err, expectedError)
		require.Len(t, mockRepository.users, 0)
	})
}

func TestGetUserByID(t *testing.T) {
	logger := zap.NewNop()

	testUser := domain.User{
		UserID:    "test-id",
		FirstName: "Billiam",
		LastName:  "Gates",
	}
	t.Run("successfully get user", func(t *testing.T) {
		t.Parallel()

		mockRepository := MockUserRepository{
			users: []*domain.User{&testUser},
		}

		service := NewUserService(&mockRepository, logger)

		user, err := service.GetUserByID(context.Background(), testUser.UserID)

		require.NoError(t, err)
		require.Equal(t, mockRepository.users[0], user)
	})

	t.Run("should propagate error on getting user", func(t *testing.T) {
		t.Parallel()
		expectedError := errors.New("get user error")
		mockRepository := MockUserRepository{
			getUserError: expectedError,
		}

		service := NewUserService(&mockRepository, logger)

		_, err := service.GetUserByID(context.Background(), "test")

		require.ErrorIs(t, err, expectedError)
	})
}

func TestDeleteUserByID(t *testing.T) {
	logger := zap.NewNop()

	testUserID := "test-id"

	t.Run("should successfully delete user", func(t *testing.T) {
		t.Parallel()

		mockRepository := MockUserRepository{
			users: []*domain.User{{UserID: testUserID}},
		}

		service := NewUserService(&mockRepository, logger)

		err := service.DeleteUserByID(context.Background(), testUserID)

		require.NoError(t, err)
		require.Empty(t, mockRepository.users)
	})

	t.Run("should propagate error if delete failed", func(t *testing.T) {
		t.Parallel()

		expectedError := errors.New("failed to delete user")
		mockRepository := MockUserRepository{
			users:       []*domain.User{{UserID: testUserID}},
			deleteError: expectedError,
		}

		service := NewUserService(&mockRepository, logger)

		err := service.DeleteUserByID(context.Background(), testUserID)

		require.ErrorIs(t, err, expectedError)
	})
}
