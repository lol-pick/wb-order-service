package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"wb-order-service/internal/models"
)

// Тесты цикла «прочитать → обработать → закоммитить».
// Главная гарантия сервиса — «хотя бы один раз»:
// оффсет коммитится только после того, как заказ сохранён в БД или надёжно отложен в DLQ.

// fakeReader — заглушка Kafka: отдаёт сообщения из очереди, запоминает закоммиченные оффсеты
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafkago.Message
	fetchErr  error
	committed []int64
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if r.fetchErr != nil {
		err := r.fetchErr
		r.mu.Unlock()
		return kafkago.Message{}, err
	}
	if len(r.msgs) > 0 {
		msg := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) commits() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

// fakeDLQ — заглушка DLQ: запоминает отправленное или возвращает ошибку
type fakeDLQ struct {
	mu      sync.Mutex
	sendErr error
	sent    []kafkago.Message
}

func (d *fakeDLQ) Send(_ context.Context, msg kafkago.Message, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sendErr != nil {
		return d.sendErr
	}
	d.sent = append(d.sent, msg)
	return nil
}

func (d *fakeDLQ) Close() error { return nil }

func (d *fakeDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.sent)
}

// funcService — сервис, поведение SaveOrder задаётся функцией
type funcService struct {
	save func() error
}

func (s *funcService) SaveOrder(_ context.Context, _ models.Order) error { return s.save() }
func (s *funcService) GetOrder(_ context.Context, _ string) (models.Order, bool, error) {
	return models.Order{}, false, nil
}
func (s *funcService) RestoreCache(_ context.Context, _ int) error { return nil }

func withOffset(msg kafkago.Message, offset int64) kafkago.Message {
	msg.Offset = offset
	return msg
}

// Заказ сохранён → оффсет закоммичен, в DLQ ничего
func TestReadAndProcess_CommitsAfterSuccess(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{withOffset(validMessage(), 5)}}
	dlq := &fakeDLQ{}
	c := &Consumer{reader: reader, dlq: dlq, service: &funcService{save: func() error { return nil }}}

	c.readAndProcess(context.Background())

	if got := reader.commits(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("expected commit of offset 5, got %v", got)
	}
	if dlq.count() != 0 {
		t.Fatalf("expected empty DLQ, got %d", dlq.count())
	}
}

// Битый JSON → DLQ, затем коммит: повторять его бессмысленно, и он не должен блокировать топик
func TestReadAndProcess_InvalidMessageGoesToDLQ(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{{Offset: 7, Value: []byte("{not json")}}}
	dlq := &fakeDLQ{}
	saved := false
	c := &Consumer{reader: reader, dlq: dlq, service: &funcService{save: func() error { saved = true; return nil }}}

	c.readAndProcess(context.Background())

	if saved {
		t.Fatal("invalid message must not reach SaveOrder")
	}
	if dlq.count() != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlq.count())
	}
	if got := reader.commits(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("expected commit of offset 7, got %v", got)
	}
}

// БД отвергла данные (ErrInvalidOrder) → DLQ и коммит, без бесконечных повторов
func TestReadAndProcess_RejectedByDBGoesToDLQ(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{withOffset(validMessage(), 8)}}
	dlq := &fakeDLQ{}
	calls := 0
	c := &Consumer{reader: reader, dlq: dlq, service: &funcService{save: func() error {
		calls++
		return fmt.Errorf("insert item 0: %w", models.ErrInvalidOrder)
	}}}

	c.readAndProcess(context.Background())

	if calls != 1 {
		t.Fatalf("expected exactly 1 save attempt, got %d", calls)
	}
	if dlq.count() != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlq.count())
	}
	if got := reader.commits(); len(got) != 1 {
		t.Fatalf("expected 1 commit, got %v", got)
	}
}

// БД недоступна, сервис останавливают посреди повторов → оффсет НЕ коммитится.
// После перезапуска Kafka отдаст это сообщение снова — заказ не потеряется.
func TestReadAndProcess_NoCommitWhenStoppedDuringRetries(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{withOffset(validMessage(), 9)}}
	dlq := &fakeDLQ{}
	c := &Consumer{reader: reader, dlq: dlq, service: &funcService{save: func() error {
		return errors.New("connection refused")
	}}}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.readAndProcess(ctx)

	if got := reader.commits(); len(got) != 0 {
		t.Fatalf("expected no commits, got %v", got)
	}
	if dlq.count() != 0 {
		t.Fatalf("transient error must not go to DLQ, got %d", dlq.count())
	}
}

// DLQ недоступна → оффсет НЕ коммитится: иначе битое сообщение исчезло бы бесследно
func TestReadAndProcess_NoCommitWhenDLQUnavailable(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{{Offset: 10, Value: []byte("{not json")}}}
	dlq := &fakeDLQ{sendErr: errors.New("kafka unavailable")}
	c := &Consumer{reader: reader, dlq: dlq, service: &funcService{save: func() error { return nil }}}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.readAndProcess(ctx)

	if got := reader.commits(); len(got) != 0 {
		t.Fatalf("expected no commits while DLQ is down, got %v", got)
	}
}

// Ошибка чтения из Kafka → ничего не обрабатываем и не коммитим
func TestReadAndProcess_FetchError(t *testing.T) {
	reader := &fakeReader{fetchErr: errors.New("broker not available")}
	saved := false
	c := &Consumer{reader: reader, dlq: &fakeDLQ{}, service: &funcService{save: func() error { saved = true; return nil }}}

	c.readAndProcess(context.Background())

	if saved {
		t.Fatal("nothing must be processed after a fetch error")
	}
	if got := reader.commits(); len(got) != 0 {
		t.Fatalf("expected no commits, got %v", got)
	}
}

// Паника в обработке не роняет консьюмер, сообщение не коммитится
func TestSafeReadAndProcess_RecoversPanic(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{withOffset(validMessage(), 11)}}
	c := &Consumer{reader: reader, dlq: &fakeDLQ{}, service: &funcService{save: func() error {
		panic("unexpected nil pointer")
	}}}

	c.safeReadAndProcess(context.Background()) // тест упал бы, если бы паника вышла наружу

	if got := reader.commits(); len(got) != 0 {
		t.Fatalf("expected no commits after panic, got %v", got)
	}
}

// Run обрабатывает все сообщения по порядку и завершается по отмене контекста
func TestRun_ProcessesInOrderAndStops(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{
		withOffset(validMessage(), 1),
		withOffset(validMessage(), 2),
		withOffset(validMessage(), 3),
	}}
	c := &Consumer{reader: reader, dlq: &fakeDLQ{}, service: &funcService{save: func() error { return nil }}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	// Ждём, пока все три сообщения будут закоммичены
	deadline := time.Now().Add(2 * time.Second)
	for len(reader.commits()) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}

	got := reader.commits()
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("expected commits [1 2 3], got %v", got)
	}
}
