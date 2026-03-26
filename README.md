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
```

## Стек технологий

- **Go** — основной язык
- **PostgreSQL** — база данных
- **Apache Kafka** — брокер сообщений
- **chi** — HTTP-роутер
- **pgx** — драйвер PostgreSQL
- **segmentio/kafka-go** — клиент Kafka
- **Docker Compose** — инфраструктура

## Структура проекта

```
wb-order-service/
├── cmd/
│   ├── service/            # Основной сервис
│   │   └── main.go
│   └── producer/           # Эмулятор отправки заказов в Kafka
│       └── main.go
├── internal/
│   ├── cache/              # In-memory кэш заказов
│   │   └── cache.go
│   ├── handler/            # HTTP-обработчики
│   │   └── handler.go
│   ├── kafka/              # Kafka consumer
│   │   └── consumer.go
│   ├── models/             # Модели данных
│   │   └── order.go
│   └── repository/         # Работа с PostgreSQL
│       └── postgres.go
├── migrations/
│   └── 001_init.sql        # SQL-миграции
├── web/
│   └── index.html          # Веб-интерфейс
├── docker-compose.yml      # Docker-инфраструктура
├── go.mod
├── go.sum
└── README.md
```

## Требования

- [Go](https://golang.org/dl/) 1.21+
- [Docker](https://www.docker.com/products/docker-desktop/) и Docker Compose

## Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone <url>
cd wb-order-service
```

### 2. Поднять инфраструктуру

```bash
docker-compose up -d
```

Это запустит:
- **PostgreSQL** (порт 5432) — с автоматическим созданием таблиц
- **Apache Kafka** (порт 9092)
- **Zookeeper** (порт 2181)

Проверить что всё поднялось:

```bash
docker ps
```

### 3. Установить зависимости

```bash
go mod tidy
```

### 4. Запустить сервис

```bash
go run cmd/service/main.go
```

### 5. Отправить тестовые заказы (в отдельном терминале)

```bash
go run cmd/producer/main.go
```

### 6. Проверить работу

**Веб-интерфейс:**

Открыть [http://localhost:8081](http://localhost:8081), ввести `order_uid` из логов и нажать "Найти".

**API:**

```bash
curl http://localhost:8081/order/<order_uid>
```

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

## Ключевые особенности

### Кэширование
- Все заказы хранятся в in-memory кэше (`sync.RWMutex` + `map`)
- При запросе сначала проверяется кэш, затем БД
- При перезапуске сервиса кэш 