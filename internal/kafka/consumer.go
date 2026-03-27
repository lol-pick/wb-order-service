package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	kafkago "github.com/segmentio/kafka-go"

	"wb-order-service/internal/config"
	"wb-order-service/internal/handler"
	"wb-order-service/internal/models"
	"wb-order-service/internal/service"
)

// Consumer - читатель сообщений из Kafka
type Consumer struct {
	reader  *kafkago.Reader
	service service.OrderService
	dlq     *DLQWriter
}

// NewConsumer создает нового Kafka-потребителя
func NewConsumer(cfg config.KafkaConfig, svc service.OrderService) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: 0,
	})

	dlq := NewDLQWriter(cfg.Brokers, cfg.DLQTopic)

	return &Consumer{
		reader:  reader,
		service: svc,
		dlq:     dlq,
	}
}

// Run запускает чтение сообщений (блокирующий метод)
func (c *Consumer) Run(ctx context.Context) {
	log.Println("Kafka consumer started, waiting for messages...")
	for {
		// Используем select для проверки ctx.Done()
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer stopped")
			return

		default:
			c.safeReadAndProcess(ctx)
		}

	}
}

// safeReadAndProcess оборачивает в recover (защита от паник)
func (c *Consumer) safeReadAndProcess(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in consumer recovered: %v\n", r)
		}
	}()
	c.readAndProcess(ctx)
}

// readAndProcess читает и обрабатывает одно сообщение
func (c *Consumer) readAndProcess(ctx context.Context) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Error reading message: %v\n", err)
		return
	}

	log.Printf("Received message from Kafka: offset = %d, key = %s\n",
		msg.Offset, string(msg.Key))

	// Обрабатываем сообщение
	if err = c.processMessage(ctx, msg); err != nil {
		handler.RecordKafkaMessage("failed")
		log.Printf("Error processing message: %v\n", err)
		// Отправляем в DLQ
		c.dlq.Send(ctx, msg, err.Error())
		handler.RecordKafkaMessage("dlq")
	} else {
		handler.RecordKafkaMessage("processed")
	}

	// Коммитим в любом случае (успех, ошибка)
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Error committing message: %v\n", err)
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafkago.Message) error {
	// 1. Парсим JSON
	var order models.Order
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	//2. Валидация - проверяем обязательные поля
	if err := validateOrder(order); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	//3. Сохраняем в БД
	err := c.service.SaveOrder(ctx, order)
	if err != nil {
		// Если дубликат - просто логируем и пропускаем
		if strings.Contains(err.Error(), "duplicate") ||
			strings.Contains(err.Error(), "already exists") {
			log.Printf("Order %s already exists, skipping\n", order.OrderUID)
			return nil
		}
		return fmt.Errorf("save to DB: %w", err)
	}

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
	if err := c.dlq.Close(); err != nil {
		log.Printf("Error closing DLQ writer: %v\n", err)
	}
	return c.reader.Close()
}
