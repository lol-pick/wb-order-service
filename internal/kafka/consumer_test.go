package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
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
