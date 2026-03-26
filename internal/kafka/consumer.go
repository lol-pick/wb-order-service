package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"wb-order-service/internal/cache"
	"wb-order-service/internal/models"
	"wb-order-service/internal/repository"
)

// Consumer - читатель сообщений из Kafka
type Consumer struct {
	reader *kafkago.Reader
	repo   *repository.PostgresRepository
	cache  *cache.OrderCache
}

// NewConsumer создает нового Kafka-потребителя
func NewConsumer(brokers []string, topic string, repo *repository.PostgresRepository, cache *cache.OrderCache) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "order-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: 0,
	})

	return &Consumer{
		reader: reader,
		repo:   repo,
		cache:  cache,
	}
}

// Run запускает чтение сообщений (блокирующий метод)
func (c *Consumer) Run(ctx context.Context) {
	log.Println("Kafka consumer started, waiting for messages...")

	for {
		//Читаем следующее сообщение из Kafka
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// Если контекст отмене - выходим (graceful shutdown)
			if ctx.Err() != nil {
				log.Println("Kafka consumer stopped")
				return
			}
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		log.Printf("Received message from Kafka: offset=%d, key=%s\n", msg.Offset, string(msg.Key))

		// Обрабатываем сообщение
		err = c.processMessage(ctx, msg)
		if err != nil {
			log.Printf("Error processing message: %v\n", err)
			// НЕ коммитим offset — сообщение будет перечитано
			continue
		}

		// Подтверждает, что сообщение обработано
		err = c.reader.CommitMessages(ctx, msg)
		if err != nil {
			log.Printf("Error committing message: %v\n", err)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafkago.Message) error {
	// 1. Парсим JSON
	var order models.Order
	err := json.Unmarshal(msg.Value, &order)
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	//2. Валидация - проверяем обязательные поля
	if err := validateOrder(order); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	//3. Сохраняем в БД
	err = c.repo.SaveOrder(ctx, order)
	if err != nil {
		return fmt.Errorf("save to DB: %w", err)
	}

	//4. Добавляем в кэш
	c.cache.Set(order)

	log.Printf("Order %s processed successfully\n", order.OrderUID)
	return nil
}

// validateOrder проверяет обязательные поля заказа
func validateOrder(order models.Order) error {
	if order.OrderUID == "" {
		return fmt.Errorf("order_uid is empty")
	}
	if order.TrackNumber == "" {
		return fmt.Errorf("track_number is empty")
	}
	if order.Entry == "" {
		return fmt.Errorf("entry is empty")
	}
	if order.CustomerID == "" {
		return fmt.Errorf("customer_id is empty")
	}
	if order.DateCreated.IsZero() {
		return fmt.Errorf("date_created is empty")
	}
	if order.Delivery.Name == "" {
		return fmt.Errorf("delivery name is empty")
	}
	if order.Payment.Transaction == "" {
		return fmt.Errorf("payment transaction is empty")
	}
	if len(order.Items) == 0 {
		return fmt.Errorf("items list is empty")
	}
	return nil
}

// Close закрывает reader
func (c *Consumer) Close() error {
	return c.reader.Close()
}
