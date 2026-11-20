// Package metrics — метрики Prometheus для слоёв, которые не относятся к HTTP:
// кэш (считает сервис) и Kafka (считает консьюмер).
//
// Раньше эти метрики жили в пакете handler, и это мешало считать их там, где происходит событие:
// сервис не может импортировать handler (handler сам импортирует сервис — получился бы цикл импортов),
// а консьюмеру Kafka незачем зависеть от HTTP-слоя.
// Пакет metrics ни от чего внутреннего не зависит, поэтому его может импортировать любой слой.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	cacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	cacheMissesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total number of cache misses (order requested from DB)",
		},
	)

	kafkaMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "Total number of Kafka messages",
		},
		[]string{"status"}, // "processed", "failed", "dlq", "retry"
	)
)

func init() {
	prometheus.MustRegister(cacheHitsTotal, cacheMissesTotal, kafkaMessagesTotal)
}

// RecordCacheHit — заказ отдан из кэша, без обращения к БД
func RecordCacheHit() {
	cacheHitsTotal.Inc()
}

// RecordCacheMiss — заказа не было в кэше, пошли в БД (найден он там или нет — неважно)
func RecordCacheMiss() {
	cacheMissesTotal.Inc()
}

// RecordKafkaMessage — итог обработки сообщения из Kafka
func RecordKafkaMessage(status string) {
	kafkaMessagesTotal.WithLabelValues(status).Inc()
}

// RegisterOrdersInCache регистрирует метрику orders_in_cache.
// GaugeFunc вызывает size в момент, когда Prometheus забирает метрики, —
// значение всегда актуальное, и кэшу не нужно ничего знать о Prometheus.
// Вызывать один раз при старте: повторная регистрация вызовет панику.
func RegisterOrdersInCache(size func() int) {
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "orders_in_cache",
			Help: "Current number of orders in cache",
		},
		func() float64 { return float64(size()) },
	))
}
