package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"wb-order-service/internal/models"
)

func TestRetry_SucceedsAfterFailures(t *testing.T) {
	calls := 0
	err := retry(context.Background(), 5, func() error {
		calls++
		if calls < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_StopsAfterMaxAttempts(t *testing.T) {
	calls := 0
	wantErr := errors.New("db down")
	err := retry(context.Background(), 2, func() error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestRetry_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// attempts == 0: без ограничения, выйти можно только по отмене контекста
	err := retry(ctx, 0, func() error { return errors.New("dlq down") })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestDecodeOrder_InvalidJSON(t *testing.T) {
	if _, err := decodeOrder([]byte("{not json")); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestDecodeOrder_MissingFields(t *testing.T) {
	if _, err := decodeOrder([]byte(`{"order_uid":"abc"}`)); err == nil {
		t.Fatal("expected validation error")
	}
}

// stubService — сервис, который первые failures вызовов SaveOrder возвращает saveErr
type stubService struct {
	calls    int
	failures int
	saveErr  error
}

func (s *stubService) SaveOrder(_ context.Context, _ models.Order) error {
	s.calls++
	if s.calls <= s.failures {
		return s.saveErr
	}
	return nil
}

func (s *stubService) GetOrder(_ context.Context, _ string) (models.Order, bool, error) {
	return models.Order{}, false, nil
}

func (s *stubService) RestoreCache(_ context.Context, _ int) error { return nil }

// validMessage — сообщение, которое проходит decodeOrder
func validMessage() kafkago.Message {
	return kafkago.Message{Value: []byte(`{
		"order_uid": "order-1", "track_number": "T1", "entry": "WBIL", "customer_id": "c1",
		"date_created": "2026-11-20T10:00:00Z",
		"delivery": {"name": "Ivan"}, "payment": {"transaction": "order-1"},
		"items": [{"chrt_id": 1}]
	}`)}
}

// Временная ошибка БД: повторяем до успеха, в DLQ не отправляем
// (dlq == nil: обращение к DLQ уронило бы тест паникой)
func TestHandle_TransientErrorRetriesUntilSuccess(t *testing.T) {
	svc := &stubService{failures: 2, saveErr: errors.New("connection refused")}
	c := &Consumer{service: svc}

	if err := c.handle(context.Background(), validMessage()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if svc.calls != 3 {
		t.Fatalf("expected 3 SaveOrder calls, got %d", svc.calls)
	}
}

// БД недоступна дольше любого лимита: консьюмер ждёт, пока его не остановят, и не трогает DLQ
func TestHandle_TransientErrorStopsOnShutdown(t *testing.T) {
	svc := &stubService{failures: 1 << 30, saveErr: errors.New("connection refused")}
	c := &Consumer{service: svc}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := c.handle(ctx, validMessage())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// Дубль — не ошибка: повторов нет
func TestHandle_DuplicateIsSkipped(t *testing.T) {
	svc := &stubService{failures: 1, saveErr: fmt.Errorf("save: %w", models.ErrOrderExists)}
	c := &Consumer{service: svc}

	if err := c.handle(context.Background(), validMessage()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if svc.calls != 1 {
		t.Fatalf("expected 1 SaveOrder call, got %d", svc.calls)
	}
}
