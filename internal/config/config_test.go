package config

import (
	"os"
	"testing"
	"time"
)

// clearEnv убирает переменные на время теста и возвращает прежние значения после него.
func clearEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if old, ok := os.LookupEnv(key); ok {
			t.Cleanup(func() { _ = os.Setenv(key, old) })
		} else {
			t.Cleanup(func() { _ = os.Unsetenv(key) })
		}
		_ = os.Unsetenv(key)
	}
}

var allKeys = []string{
	"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "POSTGRES_TIMEOUT",
	"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_GROUP_ID", "KAFKA_DLQ_TOPIC", "KAFKA_MIN_BYTES", "KAFKA_MAX_BYTES",
	"KAFKA_MAX_WAIT", "KAFKA_COMMIT_INTERVAL", "HTTP_PORT", "HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT",
	"CACHE_TTL", "CACHE_MAX_SIZE", "CACHE_RESTORE_LIMIT", "JAEGER_ENDPOINT", "OTEL_SERVICE_NAME",
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t, allKeys...)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Kafka.Topic != "orders" || cfg.Kafka.DLQTopic != "orders-dlq" {
		t.Errorf("unexpected topics: %q, %q", cfg.Kafka.Topic, cfg.Kafka.DLQTopic)
	}
	if cfg.Kafka.CommitInterval != time.Second {
		t.Errorf("expected commit interval 1s, got %v", cfg.Kafka.CommitInterval)
	}
	if cfg.Cache.TTL != 10*time.Minute || cfg.Cache.MaxSize != 10000 || cfg.Cache.RestoreLimit != 1000 {
		t.Errorf("unexpected cache config: %+v", cfg.Cache)
	}
	if cfg.HTTP.Port != 8081 {
		t.Errorf("expected port 8081, got %d", cfg.HTTP.Port)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	clearEnv(t, allKeys...)
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
	t.Setenv("CACHE_TTL", "1m")
	t.Setenv("HTTP_PORT", "9000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[1] != "kafka-2:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Kafka.Brokers)
	}
	if cfg.Cache.TTL != time.Minute {
		t.Errorf("expected TTL 1m, got %v", cfg.Cache.TTL)
	}
	if cfg.HTTP.Port != 9000 {
		t.Errorf("expected port 9000, got %d", cfg.HTTP.Port)
	}
}

// Некорректное число или длительность молча заменяются значением по умолчанию.
func TestLoad_InvalidValueFallsBackToDefault(t *testing.T) {
	clearEnv(t, allKeys...)
	t.Setenv("CACHE_MAX_SIZE", "many")
	t.Setenv("CACHE_TTL", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Cache.MaxSize != 10000 {
		t.Errorf("expected default max size, got %d", cfg.Cache.MaxSize)
	}
	if cfg.Cache.TTL != 10*time.Minute {
		t.Errorf("expected default TTL, got %v", cfg.Cache.TTL)
	}
}

func TestLoad_Validation(t *testing.T) {
	tests := []struct {
		name         string
		maxSize      string
		restoreLimit string
	}{
		{"restore limit above cache size", "100", "200"},
		{"zero cache size", "0", "0"},
		{"negative restore limit", "100", "-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t, allKeys...)
			t.Setenv("CACHE_MAX_SIZE", tt.maxSize)
			t.Setenv("CACHE_RESTORE_LIMIT", tt.restoreLimit)

			if _, err := Load(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

// Логин и пароль экранируются: спецсимволы не ломают строку подключения
func TestConnString_EscapesCredentials(t *testing.T) {
	p := PostgresConfig{User: "wb_user", Password: "p@ss:w/rd", Host: "db", Port: 5432, Database: "wb_orders"}

	got := p.ConnString()
	want := "postgres://wb_user:p%40ss%3Aw%2Frd@db:5432/wb_orders"
	if got != want {
		t.Errorf("ConnString() = %q, want %q", got, want)
	}
}
