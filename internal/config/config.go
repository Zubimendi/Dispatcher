// Package config centralizes all environment-driven configuration.
// Principle: services are stateless and configured entirely via env vars,
// so any number of replicas can start from the same image (horizontal scale).
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL      string
	RedisAddr        string
	HTTPPort         string
	WorkerConcurrency int
	WorkerPollInterval time.Duration
	HTTPClientTimeout time.Duration
	MaxDeliveryAttempts int
	ArchiveAfter      time.Duration
}

func Load() Config {
	return Config{
		DatabaseURL:          getEnv("DATABASE_URL", "postgres://dispatcher:dispatcher@localhost:5432/dispatcher?sslmode=disable"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		HTTPPort:             getEnv("HTTP_PORT", "8080"),
		WorkerConcurrency:    getEnvInt("WORKER_CONCURRENCY", 8),
		WorkerPollInterval:   getEnvDuration("WORKER_POLL_INTERVAL", 500*time.Millisecond),
		HTTPClientTimeout:    getEnvDuration("HTTP_CLIENT_TIMEOUT", 5*time.Second),
		MaxDeliveryAttempts:  getEnvInt("MAX_DELIVERY_ATTEMPTS", 8),
		ArchiveAfter:         getEnvDuration("ARCHIVE_AFTER", 90*24*time.Hour),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
