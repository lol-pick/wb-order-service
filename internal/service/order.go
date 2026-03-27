package service

import (
	"context"
	"fmt"
	"log"
	"strings"

	"wb-order-service/internal/models"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("service")

// orderService — реализация OrderService
type orderService struct {
	repo  OrderRepository
	cache OrderCache
}

// NewOrderService создаёт новый сервис заказов
func NewOrderService(repo OrderRepository, cache OrderCache) OrderService {
	return &orderService{
		repo:  repo,
		cache: cache,
	}
}

// SaveOrder сохраняет заказ в БД и кэш
func (s *orderService) SaveOrder(ctx context.Context, order models.Order) error {
	ctx, span := tracer.Start(ctx, "service.SaveOrder")
	defer span.End()

	// Сохраняем в БД
	err := s.repo.SaveOrder(ctx, order)
	if err != nil {
		return fmt.Errorf("save to repository: %w", err)
	}

	// Кэшируем
	s.cache.Set(order)

	log.Printf("Order %s saved and cached\n", order.OrderUID)
	return nil
}

// GetOrder ищет заказ: сначала кэш, потом БД
func (s *orderService) GetOrder(ctx context.Context, orderUID string) (models.Order, bool, error) {
	ctx, span := tracer.Start(ctx, "service.GetOrder")
	defer span.End()

	// 1. Ищем в кэше
	order, found := s.cache.Get(orderUID)
	if found {
		log.Printf("Cache HIT for order %s\n", orderUID)
		return order, true, nil
	}

	// 2. Ищем в БД
	log.Printf("Cache MISS for order %s, querying DB\n", orderUID)
	order, err := s.repo.GetOrder(ctx, orderUID)
	if err != nil {
		// Если ошибка - "no rows" - заказ не найден
		if strings.Contains(err.Error(), "no rows") {
			return models.Order{}, false, nil
		}
		// Иначе - реальная ошибка БД
		return models.Order{}, false, fmt.Errorf("get from repository: %w", err)
	}

	// 3. Нашли в БД — кэшируем
	s.cache.Set(order)
	return order, true, nil
}

// RestoreCache восстанавливает кэш из БД
func (s *orderService) RestoreCache(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "service.RestoreCache")
	defer span.End()

	orders, err := s.repo.GetAllOrders(ctx)
	if err != nil {
		return fmt.Errorf("get all orders: %w", err)
	}

	for _, order := range orders {
		s.cache.Set(order)
	}

	log.Printf("Cache restored: %d orders loaded from DB\n", s.cache.Size())
	return nil
}
