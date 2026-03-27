package handler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus метрики
var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	cacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	cacheMissesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	kafkaMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "Total number of Kafka messages",
		},
		[]string{"status"}, // "processed", "failed", "dlq"
	)

	ordersInCache = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "orders_in_cache",
			Help: "Current number of orders in cache",
		},
	)
)

func init() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDuration,
		cacheHitsTotal,
		cacheMissesTotal,
		kafkaMessagesTotal,
		ordersInCache,
	)
}

// PrometheusHandler возвращает handler для /metrics
func PrometheusHandler() http.Handler {
	return promhttp.Handler()
}

// metricsMiddleware — middleware для сбора HTTP метрик
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()

		// Обёртка для перехвата статус-кода
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start).Seconds()

		httpRequestsTotal.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(wrapped.statusCode),
		).Inc()

		httpRequestDuration.WithLabelValues(
			r.Method,
			r.URL.Path,
		).Observe(duration)
	})
}

// responseWriter — обёртка для перехвата статус-кода
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Функции для использования в других пакетах
func RecordCacheHit() {
	cacheHitsTotal.Inc()
}

func RecordCacheMiss() {
	cacheMissesTotal.Inc()
}

func RecordKafkaMessage(status string) {
	kafkaMessagesTotal.WithLabelValues(status).Inc()
}

func SetOrdersInCache(count int) {
	ordersInCache.Set(float64(count))
}
