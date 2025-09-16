package config

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func getExpectedConfigFromFile(t *testing.T) *Config {
	data, err := os.ReadFile("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to read config.yaml: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("Failed to unmarshal config.yaml: %v", err)
	}

	return &config
}

func TestLoadConfig(t *testing.T) {
	t.Run("With config file", func(t *testing.T) {
		viper.Reset()

		cfg, err := Load()
		assert.NoError(t, err)

		expected := getExpectedConfigFromFile(t)
		assert.Equal(t, expected, cfg)
	})

	t.Run("With environment variables", func(t *testing.T) {
		viper.Reset()

		os.Setenv("APP_APP_NAME", "env-app")
		os.Setenv("APP_SERVER_PORT", "7777")
		os.Setenv("APP_LOGGING_LEVEL", "error")
		defer func() {
			os.Unsetenv("APP_APP_NAME")
			os.Unsetenv("APP_SERVER_PORT")
			os.Unsetenv("APP_LOGGING_LEVEL")
		}()

		viper.SetConfigFile("../config.yaml")

		cfg, err := Load()
		assert.NoError(t, err)

		assert.Equal(t, "env-app", cfg.App.Name)
		assert.Equal(t, 7777, cfg.Server.Port)
		assert.Equal(t, "error", cfg.Logging.Level)
	})

	t.Run("Non existent config file", func(t *testing.T) {
		viper.Reset()

		viper.SetConfigFile("non-existent-config.yaml")

		_, err := Load()
		if err == nil {
			t.Error("Expected error when config file doesn't exist")
		}
	})

	t.Run("Default values", func(t *testing.T) {
		viper.Reset()

		tmpFile, err := os.CreateTemp("", "config-*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		tmpFile.Close()

		viper.SetConfigFile(tmpFile.Name())

		cfg, err := Load()
		assert.NoError(t, err)

		expected := getExpectedConfigFromFile(t)
		assert.Equal(t, expected.App.Name, cfg.App.Name)
		assert.Equal(t, expected.App.Version, cfg.App.Version)
		assert.Equal(t, expected.App.Environment, cfg.App.Environment)
		assert.Equal(t, expected.Server.Port, cfg.Server.Port)
		assert.Equal(t, expected.Server.Host, cfg.Server.Host)
		assert.Equal(t, expected.Logging.Level, cfg.Logging.Level)
		assert.Equal(t, expected.Logging.Format, cfg.Logging.Format)
	})
}
