package cache

import (
	"sync"
	"time"

	"wb-order-service/internal/models"
)

// cacheEntry - запись кэша с временем жизни
type cacheEntry struct {
	order     models.Order
	expiresAt time.Time
}

// OrderCache - потокобезопасный кэш заказов в памяти
type OrderCache struct {
	mu    sync.RWMutex
	items map[string]cacheEntry
	ttl   time.Duration
}

// NewOrderCache создает новый пустой кэш
func NewOrderCache(ttl time.Duration) *OrderCache {
	cache := &OrderCache{
		items: make(map[string]cacheEntry),
		ttl:   ttl,
	}

	// Запускает фоновую очистку просроченных записей
	go cache.cleanupLoop()

	return cache
}

// Set добавляет в кэш с TTL
func (c *OrderCache) Set(order models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[order.OrderUID] = cacheEntry{
		order:     order,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Get - возвращает заказ по order_uid если он есть и не просрочен
func (c *OrderCache) Get(orderUID string) (models.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, found := c.items[orderUID]
	if !found {
		return models.Order{}, false
	}

	// Проверяем TTL
	if time.Now().After(entry.expiresAt) {
		return models.Order{}, false
	}

	return entry.order, true
}

// Delete удаляет заказ из кэша
func (c *OrderCache) Delete(orderUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, orderUID)
}

// cleanupLoop периодически удаляет просроченные записи
func (c *OrderCache) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

// cleanup - вынесен в отедльный метод
func (c *OrderCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.items {
		if now.After(entry.expiresAt) {
			delete(c.items, key)
		}
	}
}

// Size - возвращает количество заказов в кэше
func (c *OrderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
