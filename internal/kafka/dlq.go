package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// DLQWriter — отправитель в Dead Letter Queue
type DLQWriter struct {
	writer *kafkago.Writer
}

// NewDLQWriter создаёт новый DLQ writer
func NewDLQWriter(brokers []string, topic string) *DLQWriter {
	return &DLQWriter{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkago.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 5 * time.Second,
		},
	}
}

// Send отправляет сообщение в DLQ с таймаутом.
// Возвращает ошибку, чтобы вызывающий код не коммитил offset, пока запись не прошла.
func (d *DLQWriter) Send(ctx context.Context, msg kafkago.Message, reason string) error {
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	dlqMsg := kafkago.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafkago.Header{
			{Key: "dlq-reason", Value: []byte(reason)},
			{Key: "original-topic", Value: []byte(msg.Topic)},
		},
	}

	if err := d.writer.WriteMessages(sendCtx, dlqMsg); err != nil {
		return fmt.Errorf("write to DLQ: %w", err)
	}
	log.Printf("Message sent to DLQ: key=%s, reason=%s\n", string(msg.Key), reason)
	return nil
}

// Close закрывает writer
func (d *DLQWriter) Close() error {
	return d.writer.Close()
}
