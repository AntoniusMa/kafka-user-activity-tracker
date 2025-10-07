package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetFullName(t *testing.T) {

	t.Run("Gets fullname with first and last name separated by space", func(t *testing.T) {
		t.Parallel()
		testUser := User{
			UserID:    "testID",
			FirstName: "Billiam",
			LastName:  "Gates",
		}

		require.Equal(t, testUser.FirstName+" "+testUser.LastName, testUser.GetFullName())
	})

	t.Run("Trims space if user has only first name", func(t *testing.T) {
		t.Parallel()
		testUser := User{
			UserID:    "testID",
			FirstName: "Billiam",
		}
		require.Equal(t, testUser.FirstName, testUser.GetFullName())
	})

	t.Run("Trims space if user has only last name", func(t *testing.T) {
		t.Parallel()
		testUser := User{
			UserID:   "testID",
			LastName: "Gates",
		}
		require.Equal(t, testUser.LastName, testUser.GetFullName())
	})
}
