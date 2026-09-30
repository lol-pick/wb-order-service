package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"wb-order-service/internal/config"
	"wb-order-service/internal/handler"
	"wb-order-service/internal/models"
	"wb-order-service/internal/service"
)

const (
	// Сколько раз пробуем сохранить заказ в БД, прежде чем отправить его в DLQ
	maxSaveAttempts = 5
	// Начальная и максимальная пауза между повторами
	retryBaseDelay = 200 * time.Millisecond
	retryMaxDelay  = 5 * time.Second
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

// readAndProcess читает одно сообщение и коммитит offset,
// только если сообщение обработано до конца.
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

	if err := c.handle(ctx, msg); err != nil {
		// Сюда попадаем только при остановке сервиса.
		// Offset не коммитим: после перезапуска Kafka отдаст сообщение снова.
		log.Printf("Message offset=%d not committed: %v\n", msg.Offset, err)
		return
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Error committing message: %v\n", err)
	}
}

// handle возвращает nil, только когда offset можно коммитить:
// заказ сохранён, это дубль или сообщение надёжно записано в DLQ.
func (c *Consumer) handle(ctx context.Context, msg kafkago.Message) error {
	order, err := decodeOrder(msg.Value)
	if err != nil {
		// Битое сообщение: повторять бессмысленно, сразу в DLQ
		handler.RecordKafkaMessage("failed")
		log.Printf("Invalid message: %v\n", err)
		return c.sendToDLQ(ctx, msg, err)
	}

	err = retry(ctx, maxSaveAttempts, func() error {
		err := c.service.SaveOrder(ctx, order)
		if errors.Is(err, models.ErrOrderExists) {
			log.Printf("Order %s already exists, skipping\n", order.OrderUID)
			return nil
		}
		return err
	})
	if err == nil {
		handler.RecordKafkaMessage("processed")
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// БД недоступна дольше, чем длятся все повторы: откладываем сообщение в DLQ
	handler.RecordKafkaMessage("failed")
	log.Printf("Save failed after %d attempts: %v\n", maxSaveAttempts, err)
	return c.sendToDLQ(ctx, msg, err)
}

// sendToDLQ повторяет отправку в DLQ, пока она не пройдёт или сервис не остановят.
// Пока DLQ недоступна, консьюмер ждёт: лучше остановиться, чем потерять сообщение.
func (c *Consumer) sendToDLQ(ctx context.Context, msg kafkago.Message, reason error) error {
	err := retry(ctx, 0, func() error {
		return c.dlq.Send(ctx, msg, reason.Error())
	})
	if err != nil {
		return err
	}
	handler.RecordKafkaMessage("dlq")
	return nil
}

// retry вызывает fn, пока она не вернёт nil, пока не кончатся попытки
// или пока не отменят ctx. attempts == 0 означает «повторять без ограничения».
// Пауза между попытками растёт вдвое, но не больше retryMaxDelay.
func retry(ctx context.Context, attempts int, fn func() error) error {
	delay := retryBaseDelay
	for i := 1; ; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		if attempts > 0 && i >= attempts {
			return err
		}
		log.Printf("Attempt %d failed: %v; retry in %s\n", i, err, delay)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		delay *= 2
		if delay > retryMaxDelay {
			delay = retryMaxDelay
		}
	}
}

// decodeOrder разбирает JSON и проверяет обязательные поля
func decodeOrder(data []byte) (models.Order, error) {
	var order models.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return models.Order{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if err := validateOrder(order); err != nil {
		return models.Order{}, fmt.Errorf("validation failed: %w", err)
	}
	return order, nil
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
