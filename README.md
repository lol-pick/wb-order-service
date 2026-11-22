# WB Order Service

![CI](https://github.com/lol-pick/wb-order-service/actions/workflows/ci.yml/badge.svg)

Микросервис для маркетплейса: получает новые заказы из Kafka, сохраняет их в PostgreSQL одной транзакцией, кэширует в памяти и отдаёт по HTTP API.
Есть метрики Prometheus, трассировка запросов в Jaeger, модульные и интеграционные тесты (покрытие 82%), нагрузочные тесты на k6.

**Результаты замеров** (подробно — в разделе [Производительность и надёжность](#производительность-и-надёжность)):
обработка 1900 заказов/с, 0 потерь и дублей при сбоях БД и перезапусках, запуск за 45 мс при 130 тыс. заказов в БД, p99 0,16 мс при 5000 RPS.

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
│   ├── 001_init.down.sql     # Откат (удаление таблиц)
│   ├── 002_orders_date_created_idx.up.sql   # Индекс для прогрева кэша
│   └── 002_orders_date_created_idx.down.sql
├── web/
│   └── index.html            # Веб-интерфейс
├── .env.example              # Шаблон переменных окружения
├── .golangci.yml             # Конфигурация линтера
├── prometheus.yml            # Конфигурация Prometheus
├── docker-compose.yml        # Docker-инфраструктура
├── go.mod
├── go.sum
└── README.md
```

## Требования

- [Go](https://go.dev/dl/) 1.25+
- [Docker](https://www.docker.com/products/docker-desktop/) и Docker Compose
- [golangci-lint](https://golangci-lint.run/usage/install/) (для проверки кода)

## Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone https://github.com/lol-pick/wb-order-service
cd wb-order-service
cp .env.example .env
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

Все настройки задаются через переменные окружения (файл `.env`, шаблон — `.env.example`):

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
KAFKA_COMMIT_INTERVAL=1s

# HTTP
HTTP_PORT=8081
HTTP_READ_TIMEOUT=10s
HTTP_WRITE_TIMEOUT=10s

# Cache
CACHE_TTL=10m
CACHE_MAX_SIZE=10000
CACHE_RESTORE_LIMIT=1000

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
- Размер ограничен `CACHE_MAX_SIZE` (по умолчанию 10 000): при переполнении вытесняется самая старая запись — она и так истекла бы первой
- Фоновая горутина `cleanupLoop` удаляет просроченные записи и останавливается через `Close()`
- При перезапуске кэш прогревается последними `CACHE_RESTORE_LIMIT` заказами (по умолчанию 1000) — двумя запросами к БД при любом объёме

### Надёжность
- **At-least-once** (сообщение будет обработано хотя бы один раз): offset в Kafka коммитится только после того, как заказ сохранён в БД или записан в DLQ; если сервис остановить посреди обработки, сообщение будет прочитано повторно
- **Временные и постоянные ошибки различаются**: если БД недоступна (нет соединения, таймаут, перезапуск), сохранение повторяется с нарастающей паузой (0,2 с → 0,4 с → … → 5 с) **без ограничения**, пока БД не вернётся; если БД отвергла сами данные (коды PostgreSQL классов `22` и `23`), повторять бессмысленно — сообщение уходит в DLQ
- **DLQ** (Dead Letter Queue): невалидные сообщения и заказы, отвергнутые БД, уходят в отдельный топик с причиной ошибки; пока DLQ недоступна, консьюмер ждёт и повторяет запись, а не теряет сообщение
- **Защита от дублей**: повторный заказ определяется по коду ошибки PostgreSQL `23505` (нарушение уникальности) и пропускается
- Запись в БД выполняется в **транзакции** (все 4 таблицы или ничего), все вставки заказа отправляются одним пакетом (`pgx.Batch`)
- **Фоновый коммит оффсетов** раз в `KAFKA_COMMIT_INTERVAL` (по умолчанию 1 с); при штатной остановке коммитится всё, при аварийном падении сообщения за последний интервал придут повторно и будут отсеяны защитой от дублей
- **Таймауты** на все операции с БД
- **recover** защищает от паник в Kafka consumer
- **Graceful shutdown** при Ctrl+C: сервис перестаёт принимать новую работу, дожидается обработки текущего сообщения и только потом закрывает соединения

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
- `kafka_messages_total{status}` — обработка Kafka-сообщений (`processed`, `failed`, `dlq`, `retry`; рост `retry` означает, что БД недоступна)
- `orders_in_cache` — количество заказов в кэше

#### Jaeger ([http://localhost:16686](http://localhost:16686))

Трейсы показывают путь запроса через все слои:
```
handler.GetOrder
  └── service.GetOrder
        ├── cache.Get (hit/miss)
        └── postgres.GetOrder (при cache miss)
```

## Производительность и надёжность

Замеры «до» сделаны на исходной версии (`main`), «после» — на текущей. Инструменты и методика — в [loadtest/README.md](loadtest/README.md).

**Условия:** MacBook Pro M4 Pro, 24 ГБ, macOS, Docker Desktop; PostgreSQL и Kafka (1 партиция) в Docker, сервис и k6 на той же машине; 3 товара в заказе; медиана трёх прогонов после разминочного.

| Замер | До | После | Что изменилось |
|---|---|---|---|
| Обработка заказов из Kafka | 820 заказов/с | **1900 заказов/с** | все вставки заказа одним пакетом `pgx.Batch` (+56%), фоновый коммит оффсетов (+39%) |
| Остановка PostgreSQL под нагрузкой (10 000 заказов) | 0 потерь, но 2 заказа в DLQ | **0 потерь, 0 в DLQ** (сбой 12 и 30 с) | временные ошибки повторяются до возвращения БД |
| Перезапуск и `kill -9` под нагрузкой | 0 потерь, простой 26–35 с | **0 потерь, 0 дублей**, простой ~5 с | быстрый прогрев кэша, graceful shutdown |
| Запуск при 130–170 тыс. заказов в БД | 66 с | **45 мс** | прогрев: 2 запроса вместо 1 + 4N (N+1), только последние 1000 заказов |
| GET /order, p99 при 5000 RPS | — | **0,16 мс** с кэшем / 8,4 мс без | — |

Прогрев 10 000 заказов занимает 150 мс: в пересчёте на заказ это примерно в 34 раза быстрее старого способа (0,015 мс против 0,51 мс).

Время ответа GET /order (1000 разных заказов, ошибок 0%):

| Нагрузка | С кэшем p50 / p99 | Без кэша p50 / p99 |
|---|---|---|
| 500 RPS | 0,28 / 1,43 мс | 1,37 / 2,96 мс |
| 3000 RPS | 0,08 / 0,16 мс | 0,78 / 3,69 мс |
| 5000 RPS | 0,05 / 0,16 мс | 1,04 / 8,44 мс |

Без кэша каждый запрос — 4 запроса к PostgreSQL (20 000 в секунду при 5000 RPS), и с ростом нагрузки растёт хвост задержек; с кэшем p99 не меняется.

## Тестирование

Покрытие пакетов `internal/` — **82%** (`go test -cover ./internal/...` с поднятым PostgreSQL).

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

Тестируют взаимодействие с PostgreSQL: подключение, CRUD, дубликаты, прогрев кэша, классификацию ошибок.

### Консьюмер Kafka и HTTP

```bash
go test ./internal/kafka/ ./internal/handler/ -v
```

- **Консьюмер** — цикл «прочитать → обработать → закоммитить» с заглушками Kafka и DLQ: оффсет коммитится только после сохранения заказа или записи в DLQ; при остановке во время повторов и при недоступной DLQ коммита нет; паника не роняет консьюмер; порядок обработки сохраняется
- **HTTP** — запросы через настоящий роутер (`httptest`): 200 / 404 / 500 без утечки текста внутренней ошибки, `/health`, метка `path` в метриках — шаблон маршрута, а не реальный `order_uid`

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
