package cache

import (
	"sync"
	"wb-order-service/internal/models"
)

// OrderCache - потокобезопасный кэш заказов в памяти
type OrderCache struct {
	mu    sync.RWMutex
	items map[string]models.Order
}

// NewOrderCache создает новый пустой кэш
func NewOrderCache() *OrderCache {
	return &OrderCache{
		items: make(map[string]models.Order),
	}
}

// Set добавляет или обновляет заказ в кэше
func (c *OrderCache) Set(order models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[order.OrderUID] = order
}

// Get - возвращает заказ по order_uid
// Второй возвращаемый параметр - найден ли заказ
func (c *OrderCache) Get(orderUID string) (models.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	order, found := c.items[orderUID]
	return order, found
}

// Size - возвращает количество заказов в кэше
func (c *OrderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
