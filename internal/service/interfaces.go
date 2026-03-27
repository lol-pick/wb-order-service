package service

import (
	"context"

	"wb-order-service/internal/models"
)

// OrderRepository — интерфейс для работы с БД
type OrderRepository interface {
	SaveOrder(ctx context.Context, order models.Order) error
	GetOrder(ctx context.Context, orderUID string) (models.Order, error)
	GetAllOrders(ctx context.Context) ([]models.Order, error)
	Close()
}

// OrderCache interface — интерфейс для работы с кэшем
type OrderCache interface {
	Set(order models.Order)
	Get(orderUID string) (models.Order, bool)
	Delete(orderUID string)
	Size() int
}

// OrderService — интерфейс бизнес-логики
type OrderService interface {
	SaveOrder(ctx context.Context, order models.Order) error
	GetOrder(ctx context.Context, orderUID string) (models.Order, bool, error)
	RestoreCache(ctx context.Context) error
}
