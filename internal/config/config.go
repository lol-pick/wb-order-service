package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config — полная конфигурация приложения
type Config struct {
	Postgres PostgresConfig
	Kafka    KafkaConfig
	HTTP     HTTPConfig
	Cache    CacheConfig
	Tracing  TracingConfig
}

// PostgresConfig — настройки подключения к БД
type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Timeout  time.Duration
}

// KafkaConfig — настройки Kafka
type KafkaConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	DLQTopic string
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration
	// CommitInterval — как часто подтверждать оффсеты в фоне (0 — синхронно после каждого сообщения)
	CommitInterval time.Duration
}

// HTTPConfig — настройки HTTP-сервера
type HTTPConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// CacheConfig — настройки кэша
type CacheConfig struct {
	TTL time.Duration
	// MaxSize — максимум заказов в кэше; при переполнении вытесняется самый старый
	MaxSize int
	// RestoreLimit — сколько последних заказов загрузить в кэш при старте (0 — не прогревать)
	RestoreLimit int
}

type TracingConfig struct {
	JaegerEndpoint string
	ServiceName    string
}

// ConnString возвращает строку подключения к PostgreSQL.
// Собираем через net/url: логин и пароль экранируются.
func (p PostgresConfig) ConnString() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(p.User, p.Password),
		Host:   fmt.Sprintf("%s:%d", p.Host, p.Port),
		Path:   "/" + p.Database,
	}
	return u.String()
}

// Load загружает конфигурацию из переменных окружения
func Load() (*Config, error) {
	cfg := &Config{
		Postgres: PostgresConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     getEnvAsInt("POSTGRES_PORT", 5432),
			User:     getEnv("POSTGRES_USER", "wb_user"),
			Password: getEnv("POSTGRES_PASSWORD", "wb_password"),
			Database: getEnv("POSTGRES_DB", "wb_orders"),
			Timeout:  getEnvAsDuration("POSTGRES_TIMEOUT", 5*time.Second),
		},
		Kafka: KafkaConfig{
			Brokers:        getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:          getEnv("KAFKA_TOPIC", "orders"),
			GroupID:        getEnv("KAFKA_GROUP_ID", "order-service"),
			DLQTopic:       getEnv("KAFKA_DLQ_TOPIC", "orders-dlq"),
			MinBytes:       getEnvAsInt("KAFKA_MIN_BYTES", 1),
			MaxBytes:       getEnvAsInt("KAFKA_MAX_BYTES", 10485760),
			MaxWait:        getEnvAsDuration("KAFKA_MAX_WAIT", 1*time.Second),
			CommitInterval: getEnvAsDuration("KAFKA_COMMIT_INTERVAL", 1*time.Second),
		},
		HTTP: HTTPConfig{
			Port:         getEnvAsInt("HTTP_PORT", 8081),
			ReadTimeout:  getEnvAsDuration("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout: getEnvAsDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		},
		Cache: CacheConfig{
			TTL:          getEnvAsDuration("CACHE_TTL", 10*time.Minute),
			MaxSize:      getEnvAsInt("CACHE_MAX_SIZE", 10000),
			RestoreLimit: getEnvAsInt("CACHE_RESTORE_LIMIT", 1000),
		},
		Tracing: TracingConfig{
			JaegerEndpoint: getEnv("JAEGER_ENDPOINT", "http://localhost:4318"),
			ServiceName:    getEnv("OTEL_SERVICE_NAME", "order-service"),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate проверяет значения, при которых сервис заведомо работает неправильно
func (c *Config) validate() error {
	if c.Cache.MaxSize <= 0 {
		return fmt.Errorf("CACHE_MAX_SIZE must be > 0, got %d", c.Cache.MaxSize)
	}
	if c.Cache.RestoreLimit < 0 || c.Cache.RestoreLimit > c.Cache.MaxSize {
		// Иначе прогрев сам вытеснил бы из кэша часть только что загруженных заказов
		return fmt.Errorf("CACHE_RESTORE_LIMIT must be in [0, CACHE_MAX_SIZE=%d], got %d",
			c.Cache.MaxSize, c.Cache.RestoreLimit)
	}
	return nil
}

// getEnv читает переменную окружения с дефолтным значением
func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// getEnvAsInt читает переменную окружения как число
func getEnvAsInt(key string, defaultVal int) int {
	strVal := getEnv(key, "")
	if strVal == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(strVal)
	if err != nil {
		return defaultVal
	}
	return val
}

// getEnvAsDuration читает переменную окружения как Duration
func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	strVal := getEnv(key, "")
	if strVal == "" {
		return defaultVal
	}
	val, err := time.ParseDuration(strVal)
	if err != nil {
		return defaultVal
	}
	return val
}

// getEnvAsSlice читает переменную окружения как срез строк
func getEnvAsSlice(key string, defaultVal []string) []string {
	strVal := getEnv(key, "")
	if strVal == "" {
		return defaultVal
	}
	return strings.Split(strVal, ",")
}
