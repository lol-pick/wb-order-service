# WB Order Service

Микросервис для обработки и отображения данных о заказах.

## Архитектура

```
Kafka Producer (эмулятор)
        │
        ▼
   ┌─────────┐       ┌─────────────┐
   │  Kafka   │─────▶│ Go Service  │
   └─────────┘       │             │
                     │  ┌───────┐  │     ┌────────────┐
                     │  │ Cache │  │────▶│ PostgreSQL │
                     │  └───────┘  │     └────────────┘
                     │             │
                     │ HTTP Server │◀──── Browser (HTML/JS)
                     └─────────────┘
         │
    ┌────┴────┐         ┌────────────┐      ┌────────┐
    │orders-dlq│        │ Prometheus │      │ Jaeger │
    └─────────┘         └────────────┘      └────────┘
```

### Слои приложения (Clean Architecture)

```
Handler (HTTP)
    │
    ▼
Service (бизнес-логика, интерфейсы)
    │
    ├──▶ Repository (PostgreSQL)
    └──▶ Cache (in-memory, TTL)
```

## Стек технологий

- **Go** — основной язык
- **PostgreSQL** — база данных
- **Apache Kafka** — брокер сообщений
- **chi** — HTTP-роутер
- **pgx** — драйвер PostgreSQL
- **segmentio/kafka-go** — клиент Kafka
- **Prometheus** — сбор и хранение метрик
- **Jaeger** — распределённый трейсинг (OpenTelemetry)
- **Docker Compose** — инфраструктура
- **golangci-lint** — линтер

## Структура проекта

```
wb-order-service/
├── cmd/
│   ├── service/              # Основной сервис
│   │   └── main.go
│   └── producer/             # Эмулятор отправки заказов в Kafka
│       └── main.go
├── internal/
│   ├── cache/                # In-memory кэш с TTL и инвалидацией
│   │   └── cache.go
│   ├── config/               # Конфигурация из переменных окружения
│   │   └── config.go
│   ├── handler/              # HTTP-обработчики + Prometheus метрики
│   │   ├── handler.go
│   │   └── metrics.go
│   ├── kafka/                # Kafka consumer + DLQ
│   │   ├── consumer.go
│   │   └── dlq.go
│   ├── models/               # Модели данных
│   │   └── order.go
│   ├── repository/           # Работа с PostgreSQL + интеграционные тесты
│   │   ├── postgres.go
│   │   └── postgres_test.go
│   ├── service/              # Бизнес-логика + интерфейсы + юнит-тесты
│   │   ├── interfaces.go
│   │   ├── order.go
│   │   └── order_test.go
│   └── tracing/              # OpenTelemetry трейсинг (Jaeger)
│       └── tracing.go
├── migrations/
│   ├── 001_init.up.sql       # Создание таблиц
│   └── 001_init.down.sql     # Откат (удаление таблиц)
├── web/
│   └── index.html            # Веб-интерфейс
├── .env                      # Переменные окружения
├── .golangci.yml             # Конфигурация линтера
├── prometheus.yml            # Конфигурация Prometheus
├── docker-compose.yml        # Docker-инфраструктура
├── go.mod
├── go.sum
└── README.md
```

## Требования

- [Go](https://golang.org/dl/) 1.21+
- [Docker](https://www.docker.com/products/docker-desktop/) и Docker Compose
- [golangci-lint](https://golangci-lint.run/usage/install/) (для проверки кода)

## Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone <url>
cd wb-order-service
```

### 2. Поднять инфраструктуру

```bash
docker-compose up -d
sleep 20
```

Это запустит:
- **PostgreSQL** (порт 5432) — с автоматическим созданием таблиц
- **Apache Kafka** (порт 9092) + **Zookeeper** (порт 2181)
- **Prometheus** (порт 9090) — сбор метрик
- **Jaeger** (порт 16686) — трейсинг

### 3. Создать Kafka-топики

```bash
docker exec -it wb-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 --topic orders --partitions 1 --replication-factor 1

docker exec -it wb-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 --topic orders-dlq --partitions 1 --replication-factor 1
```

### 4. Установить зависимости

```bash
go mod tidy
```

### 5. Запустить сервис

```bash
go run cmd/service/main.go
```

### 6. Отправить тестовые заказы (в отдельном терминале)

```bash
go run cmd/producer/main.go
```

### 7. Проверить работу

| Что проверить | URL |
|---------------|-----|
| Веб-интерфейс | [http://localhost:8081](http://localhost:8081) |
| API заказа | `curl http://localhost:8081/order/<order_uid>` |
| Health check | `curl http://localhost:8081/health` |
| Prometheus метрики | `curl http://localhost:8081/metrics` |
| Prometheus UI | [http://localhost:9090](http://localhost:9090) |
| Jaeger UI | [http://localhost:16686](http://localhost:16686) |

## API

### GET /order/{order_uid}

Возвращает данные заказа по его ID.

**Успешный ответ (200):**

```json
{
  "order_uid": "order-0-1705312345678",
  "track_number": "TRACK42567",
  "entry": "WBIL",
  "delivery": {
    "name": "Ivan Petrov",
    "phone": "+70001234567",
    "city": "Moscow"
  },
  "payment": {
    "transaction": "order-0-1705312345678",
    "amount": 5432,
    "currency": "RUB"
  },
  "items": [
    {
      "name": "Sneakers",
      "brand": "Nike",
      "price": 3500
    }
  ]
}
```

**Заказ не найден (404):**

```json
{"error": "order not found"}
```

### GET /health

```json
{"status": "ok"}
```

### GET /metrics

Prometheus-метрики в стандартном формате.

## Конфигурация

Все настройки задаются через переменные окружения (файл `.env`):

```env
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=wb_user
POSTGRES_PASSWORD=wb_password
POSTGRES_DB=wb_orders
POSTGRES_TIMEOUT=30s

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=orders
KAFKA_GROUP_ID=order-service
KAFKA_DLQ_TOPIC=orders-dlq
KAFKA_MIN_BYTES=1
KAFKA_MAX_BYTES=10485760
KAFKA_MAX_WAIT=1s

# HTTP
HTTP_PORT=8081
HTTP_READ_TIMEOUT=10s
HTTP_WRITE_TIMEOUT=10s

# Cache
CACHE_TTL=10m

# Tracing
JAEGER_ENDPOINT=http://localhost:4318
OTEL_SERVICE_NAME=order-service
```

## Ключевые особенности

### Clean Architecture
- **Handler** — HTTP-слой, зависит только от интерфейса `OrderService`
- **Service** — бизнес-логика (кэш + БД + валидация)
- **Repository** — работа с PostgreSQL
- **Cache** — in-memory кэш с TTL
- Все зависимости описаны через **интерфейсы** (`interfaces.go`)

### Кэширование с инвалидацией (TTL)
- Заказы хранятся в in-memory кэше (`sync.RWMutex` + `map`)
- Записи автоматически инвалидируются по TTL
- Фоновая горутина `cleanupLoop` удаляет просроченные записи
- При перезапуске сервиса кэш восстанавливается из БД

### Надёжность
- **At Least Once** гарантия: offset коммитится только после обработки
- Запись в БД выполняется в **транзакции** (все 4 таблицы или ничего)
- **DLQ** (Dead Letter Queue): невалидные сообщения сохраняются для анализа
- **Таймауты** на все операции с БД
- **recover** защищает от паник в Kafka consumer
- **Graceful shutdown** при Ctrl+C

### База данных (4 нормализованные таблицы)

```
orders (order_uid PK)
  ├── deliveries (1:1, FK → orders, CASCADE)
  ├── payments   (1:1, FK → orders, CASCADE)
  └── items      (1:N, FK → orders, CASCADE)
```

Миграции: `up` для создания, `down` для отката.

### Мониторинг

#### Prometheus ([http://localhost:9090](http://localhost:9090))

Доступные метрики:
- `http_requests_total{method, path, status}` — HTTP-запросы
- `http_request_duration_seconds` — время ответа (гистограмма)
- `cache_hits_total` / `cache_misses_total` — эффективность кэша
- `kafka_messages_total{status}` — обработка Kafka-сообщений
- `orders_in_cache` — количество заказов в кэше

#### Jaeger ([http://localhost:16686](http://localhost:16686))

Трейсы показывают путь запроса через все слои:
```
handler.GetOrder
  └── service.GetOrder
        ├── cache.Get (hit/miss)
        └── postgres.GetOrder (при cache miss)
```

## Тестирование

### Юнит-тесты (с моками)

```bash
go test ./internal/service/ -v
```

Тестируют бизнес-логику изолированно. Используют моки репозитория и кэша.

Покрытые сценарии:
- Успешное сохранение заказа
- Ошибка БД при сохранении
- Обработка дубликатов
- Cache HIT / Cache MISS
- Заказ не найден
- Восстановление кэша (с данными и пустого)

### Интеграционные тесты (с реальной БД)

```bash
go test ./internal/repository/ -v
```

Тестируют взаимодействие с PostgreSQL: подключение, CRUD, дубликаты.

### Все тесты

```bash
go test ./... -v
```

### Линтер

```bash
golangci-lint run ./...
```

## Остановка

**Сервис:**

```
Ctrl+C
```

**Инфраструктура:**

```bash
docker-compose down       # остановить контейнеры
docker-compose down -v    # остановить + удалить данные
```