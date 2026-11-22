package cache

import (
	"container/list"
	"sync"
	"time"

	"wb-order-service/internal/models"
)

// cleanupInterval — как часто фоновая горутина удаляет просроченные записи
const cleanupInterval = time.Minute

// cacheEntry - запись кэша с временем жизни
type cacheEntry struct {
	order     models.Order
	expiresAt time.Time
	// elem — позиция ключа в очереди c.order; нужна, чтобы удалить ключ из очереди за O(1)
	elem *list.Element
}

// OrderCache - потокобезопасный кэш заказов в памяти с TTL и ограничением размера.
//
// Ключи лежат в очереди в порядке записи. TTL у всех записей одинаковый,
// поэтому порядок записи совпадает с порядком истечения: в начале очереди
// всегда запись, которая истечёт раньше всех. Отсюда два свойства:
//   - при переполнении вытесняем первую запись — ту, что всё равно умрёт первой;
//   - очистка идёт с начала очереди и останавливается на первой живой записи,
//     а не обходит всю map.
type OrderCache struct {
	mu      sync.RWMutex
	items   map[string]*cacheEntry
	order   *list.List // ключи (order_uid) от самой старой записи к самой новой
	ttl     time.Duration
	maxSize int

	done      chan struct{} // закрывается в Close — сигнал фоновой горутине завершиться
	closeOnce sync.Once     // защищает от паники при повторном close(done)
}

// NewOrderCache создает новый пустой кэш.
// maxSize — максимум записей; при переполнении вытесняется самая старая (maxSize <= 0 — без лимита).
// После использования нужно вызвать Close, чтобы остановить фоновую очистку.
func NewOrderCache(ttl time.Duration, maxSize int) *OrderCache {
	cache := &OrderCache{
		items:   make(map[string]*cacheEntry),
		order:   list.New(),
		ttl:     ttl,
		maxSize: maxSize,
		done:    make(chan struct{}),
	}

	// Запускает фоновую очистку просроченных записей
	go cache.cleanupLoop()

	return cache
}

// Set добавляет в кэш с TTL
func (c *OrderCache) Set(order models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := time.Now().Add(c.ttl)

	// Ключ уже есть: обновляем данные и TTL, переносим ключ в конец очереди —
	// теперь эта запись истекает позже всех
	if e, ok := c.items[order.OrderUID]; ok {
		e.order = order
		e.expiresAt = expiresAt
		c.order.MoveToBack(e.elem)
		return
	}

	// Новый ключ, а места нет: вытесняем самую старую запись
	if c.maxSize > 0 && len(c.items) >= c.maxSize {
		c.removeOldest()
	}

	elem := c.order.PushBack(order.OrderUID)
	c.items[order.OrderUID] = &cacheEntry{
		order:     order,
		expiresAt: expiresAt,
		elem:      elem,
	}
}

// Get - возвращает заказ по order_uid если он есть и не просрочен.
// Только читает, поэтому берёт RLock: параллельные Get не блокируют друг друга.
func (c *OrderCache) Get(orderUID string) (models.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, found := c.items[orderUID]
	if !found {
		return models.Order{}, false
	}

	// Проверяем TTL: запись могла истечь, но фоновая очистка ещё не дошла до неё
	if time.Now().After(entry.expiresAt) {
		return models.Order{}, false
	}

	return entry.order, true
}

// Delete удаляет заказ из кэша
func (c *OrderCache) Delete(orderUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.items[orderUID]; ok {
		c.order.Remove(e.elem)
		delete(c.items, orderUID)
	}
}

// Size - возвращает количество заказов в кэше
func (c *OrderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Close останавливает фоновую очистку. Повторный вызов безопасен.
func (c *OrderCache) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
	})
}

// cleanupLoop периодически удаляет просроченные записи, пока не вызван Close
func (c *OrderCache) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

// cleanup удаляет просроченные записи с начала очереди
// и останавливается на первой живой: дальше все записи новее
func (c *OrderCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for elem := c.order.Front(); elem != nil; {
		uid := elem.Value.(string)
		if !now.After(c.items[uid].expiresAt) {
			break
		}
		next := elem.Next() // запоминаем до Remove: после удаления elem.Next() вернёт nil
		c.order.Remove(elem)
		delete(c.items, uid)
		elem = next
	}
}

// removeOldest удаляет первую запись очереди. Вызывать под c.mu.Lock.
func (c *OrderCache) removeOldest() {
	front := c.order.Front()
	if front == nil {
		return
	}
	uid := front.Value.(string)
	c.order.Remove(front)
	delete(c.items, uid)
}
