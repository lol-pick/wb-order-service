package kafka

import (
	"context"
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

// Send отправляет сообщение в DLQ с таймаутом
func (d *DLQWriter) Send(ctx context.Context, msg kafkago.Message, reason string) {
	// Таймаут 5 секунд на отправку
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

	err := d.writer.WriteMessages(sendCtx, dlqMsg)
	if err != nil {
		// Если не удалось отправить в DLQ — просто логируем
		log.Printf("Failed to send message to DLQ (non-critical): %v\n", err)
	} else {
		log.Printf("Message sent to DLQ: key=%s, reason=%s\n", string(msg.Key), reason)
	}
}

// Close закрывает writer
func (d *DLQWriter) Close() error {
	return d.writer.Close()
}
