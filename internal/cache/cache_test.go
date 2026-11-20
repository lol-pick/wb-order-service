package cache

import (
	"fmt"
	"testing"
	"time"

	"wb-order-service/internal/models"
)

func testOrder(uid string) models.Order {
	return models.Order{OrderUID: uid}
}

// При переполнении вытесняется самая старая запись
func TestCache_EvictsOldestWhenFull(t *testing.T) {
	c := NewOrderCache(time.Minute, 2)
	defer c.Close()

	c.Set(testOrder("a"))
	c.Set(testOrder("b"))
	c.Set(testOrder("c")) // места нет — уходит "a"

	if _, ok := c.Get("a"); ok {
		t.Errorf("expected 'a' to be evicted")
	}
	for _, uid := range []string{"b", "c"} {
		if _, ok := c.Get(uid); !ok {
			t.Errorf("expected %q in cache", uid)
		}
	}
	if c.Size() != 2 {
		t.Errorf("expected size 2, got %d", c.Size())
	}
}

// Повторный Set того же ключа не занимает новое место и делает запись «самой новой»
func TestCache_UpdateMovesKeyToBack(t *testing.T) {
	c := NewOrderCache(time.Minute, 2)
	defer c.Close()

	c.Set(testOrder("a"))
	c.Set(testOrder("b"))
	c.Set(testOrder("a")) // обновили "a" — теперь самая старая "b"
	c.Set(testOrder("c")) // вытесняется "b"

	if _, ok := c.Get("b"); ok {
		t.Errorf("expected 'b' to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Errorf("expected 'a' in cache")
	}
}

// Просроченная запись не отдаётся, даже если очистка ещё не прошла
func TestCache_GetExpired(t *testing.T) {
	c := NewOrderCache(time.Millisecond, 0)
	defer c.Close()

	c.Set(testOrder("a"))
	time.Sleep(5 * time.Millisecond)

	if _, ok := c.Get("a"); ok {
		t.Errorf("expected expired entry to be hidden")
	}
}

// Очистка удаляет только просроченные записи
func TestCache_CleanupRemovesOnlyExpired(t *testing.T) {
	c := NewOrderCache(50*time.Millisecond, 0)
	defer c.Close()

	for i := 0; i < 3; i++ {
		c.Set(testOrder(fmt.Sprintf("old-%d", i)))
	}
	time.Sleep(60 * time.Millisecond)
	c.Set(testOrder("fresh"))

	c.cleanup()

	if c.Size() != 1 {
		t.Fatalf("expected 1 entry after cleanup, got %d", c.Size())
	}
	if _, ok := c.Get("fresh"); !ok {
		t.Errorf("expected 'fresh' to survive cleanup")
	}
}

func TestCache_Delete(t *testing.T) {
	c := NewOrderCache(time.Minute, 0)
	defer c.Close()

	c.Set(testOrder("a"))
	c.Delete("a")
	c.Delete("missing") // удаление отсутствующего ключа — не паника

	if c.Size() != 0 {
		t.Errorf("expected empty cache, got %d", c.Size())
	}
}

// Повторный Close не паникует
func TestCache_CloseTwice(t *testing.T) {
	c := NewOrderCache(time.Minute, 0)
	c.Close()
	c.Close()
}
