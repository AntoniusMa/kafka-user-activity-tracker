package domain

import (
	"context"
	"strings"
)

type User struct {
	UserID    string
	FirstName string
	LastName  string
}

func (u *User) GetFullName() string {
	return strings.Trim(u.FirstName+" "+u.LastName, " ")
}

type UserRepository interface {
	Create(ctx context.Context, user *User) (*User, error)
	GetByID(ctx context.Context, id string) (*User, error)
	DeleteByID(ctx context.Context, id string) error
}
