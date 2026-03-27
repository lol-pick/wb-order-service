package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"wb-order-service/internal/models"
)

// --- Моки ---

// mockRepo - мок репозитория
type mockRepo struct {
	orders map[string]models.Order
	// Настраиваемые ошибки
	saveError error
	getError  error
}

// newMockRepo - конструктор мока
func newMockRepo() *mockRepo {
	return &mockRepo{orders: make(map[string]models.Order)}
}

// SaveOrder - имитирует Insert в БД
func (m *mockRepo) SaveOrder(_ context.Context, order models.Order) error {
	// Если настроена ошибка - возвращаем ее
	if m.saveError != nil {
		return m.saveError
	}
	// Имитируем Primary Key constraint
	if _, exists := m.orders[order.OrderUID]; exists {
		return fmt.Errorf("duplicate key: %s", order.OrderUID)
	}

	// Сохраняем в map (insert bd)
	m.orders[order.OrderUID] = order
	return nil
}

// GetOrder - имитурет поиск в бд по uid
func (m *mockRepo) GetOrder(_ context.Context, uid string) (models.Order, error) {
	if m.getError != nil {
		return models.Order{}, m.getError
	}
	order, ok := m.orders[uid]
	if !ok {
		return models.Order{}, fmt.Errorf("order not found: %s", uid)
	}
	return order, nil
}

// GetAllOrders - имитирует поиск в бд
func (m *mockRepo) GetAllOrders(_ context.Context) ([]models.Order, error) {
	var orders []models.Order
	for _, o := range m.orders {
		orders = append(orders, o)
	}
	return orders, nil
}

// Close - ничего не делает, тк нет реального соединения
func (m *mockRepo) Close() {}

// -------------------------------------------

// mockCache - мок кэша
type mockCache struct {
	items map[string]models.Order
}

func newMockCache() *mockCache {
	return &mockCache{
		items: make(map[string]models.Order),
	}
}

func (m *mockCache) Set(order models.Order) {
	m.items[order.OrderUID] = order
}

func (m *mockCache) Get(uid string) (models.Order, bool) {
	order, ok := m.items[uid]
	return order, ok
}

func (m *mockCache) Delete(uid string) {
	delete(m.items, uid)
}

func (m *mockCache) Size() int {
	return len(m.items)
}

// Вспомогательные функции

// createTestOrder - создает тестовый заказ с заданными UID
func createTestOrder(uid string) models.Order {
	return models.Order{
		OrderUID:        uid,
		TrackNumber:     "TESTTRACK",
		Entry:           "WBIL",
		CustomerID:      "customer-1",
		DeliveryService: "meest",
		Locale:          "en",
		ShardKey:        "9",
		SmID:            99,
		DateCreated:     time.Now(),
		OofShard:        "1",
		Delivery: models.Delivery{
			Name:    "Test User",
			Phone:   "+71234567890",
			Zip:     "123456",
			City:    "Moscow",
			Address: "Test Street 1",
			Region:  "Central",
			Email:   "test@test.com",
		},
		Payment: models.Payment{
			Transaction:  uid,
			Currency:     "RUB",
			Provider:     "wbpay",
			Amount:       1000,
			PaymentDT:    time.Now().Unix(),
			Bank:         "alpha",
			DeliveryCost: 500,
			GoodsTotal:   500,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      12345,
				TrackNumber: "TESTTRACK",
				Price:       500,
				RID:         "test-rid",
				Name:        "Test Item",
				Sale:        10,
				Size:        "M",
				TotalPrice:  450,
				NmID:        67890,
				Brand:       "TestBrand",
				Status:      202,
			},
		},
	}
}

// Юнит-тесы

// Тест: успешное сохранение заказа
func TestSaveOrder_Sucess(t *testing.T) {
	// Подготовка
	name := "test-save-001"
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)
	order := createTestOrder(name)

	// Действие
	err := svc.SaveOrder(context.Background(), order)

	// Проверка
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Проверяем что заказ попал в мокБД
	if _, exists := repo.orders[name]; !exists {
		t.Errorf("Order was not saved to repository")
	}

	// Проверяем что заказ попал в кэш
	if _, exists := cache.items[name]; !exists {
		t.Error("Order was not saved to cache")
	}
}

// Тест: ошибка при сохранении в БД
func TestSaveOrder_RepoError(t *testing.T) {
	// Подготовка
	repo := newMockRepo()
	repo.saveError = fmt.Errorf("database connection lost")
	cache := newMockCache()
	svc := NewOrderService(repo, cache)
	name := "test-save-002"
	order := createTestOrder(name)

	// Выполнение
	err := svc.SaveOrder(context.Background(), order)

	// Проверка
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Заказ НЕ должен быть в кэше (БД упал => кэш не обновляем)
	if _, exists := cache.items[name]; exists {
		t.Errorf("Order should not be cached when repo fails")
	}
}

// Тест дубликат заказа
func TestSaveOrder_Duplication(t *testing.T) {
	//Подготовка
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)
	name := "test-dup-001"
	order := createTestOrder(name)

	// Выполнение
	err := svc.SaveOrder(context.Background(), order)
	if err != nil {
		t.Fatalf("First save failed: %v", err)
	}
	// Второй раз сохраняем
	err = svc.SaveOrder(context.Background(), order)

	// Проверка - должна быть ошибка дубликата
	if err == nil {
		t.Fatalf("Expected dupliicate error, got hil")
	}
}

// Тест получение заказа из кэша
func TestGetOrder_CacheHit(t *testing.T) {
	// Подготовка
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)

	name := "test-get-001"
	order := createTestOrder(name)
	cache.Set(order)

	// Выполнение
	result, found, err := svc.GetOrder(context.Background(), name)

	// Проверка
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !found {
		t.Fatalf("Expected order to be found")
	}
	if result.OrderUID != name {
		t.Errorf("Expected UID %s, got %s", name, result.OrderUID)
	}
	if result.Delivery.Name != "Test User" {
		t.Errorf("Expected delivery name 'Test User', got '%s'", result.Delivery.Name)
	}
}

// Тест: полученный заказ из БД, cache miss => db hit
func TestGetOrder_CacheMiss_DBHit(t *testing.T) {
	// Подготовка
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)

	name := "test-get-002"
	order := createTestOrder(name)
	repo.orders[name] = order

	// Выполнение
	result, found, err := svc.GetOrder(context.Background(), name)

	// Проверка
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !found {
		t.Fatalf("Expected order to be found to be in DB")
	}
	if result.OrderUID != name {
		t.Errorf("Expected UID %s, got %s", name, result.OrderUID)
	}

	// Проверяем что после DB HIT заказ попал в кэш
	if _, exists := cache.items[name]; !exists {
		t.Errorf("Order should be cached after DB hit")
	}
}

// Тест - заказ не найден нигде
func TestGetOrder_NotFound(t *testing.T) {
	// Подготовка
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)

	// Выполнение - ищем несущствующий заказ
	_, _, err := svc.GetOrder(context.Background(), "nonexistent")

	// Проверка - должна быть ошибка
	if err == nil {
		t.Fatalf("expected error for nonexistent order")
	}
}

// Тест восстановление кэша из бд
func TestRestoreCache(t *testing.T) {
	// Подготовка
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)

	// Кладем 3 заказа в бд
	repo.orders["order-1"] = createTestOrder("order-1")
	repo.orders["order-2"] = createTestOrder("order-2")
	repo.orders["order-3"] = createTestOrder("order-3")

	// Выполенние
	err := svc.RestoreCache(context.Background())

	// Проверка
	if err != nil {
		t.Fatalf("RestoreCache failed: %v", err)
	}
	if cache.Size() != 3 {
		t.Errorf("Expected 3 orders in cache, got %d", cache.Size())
	}
	// Проверяем что каждый заказ в кэше
	for _, uid := range []string{"order-1", "order-2", "order-3"} {
		if _, exists := cache.items[uid]; !exists {
			t.Errorf("Order %s not found in cache after restore", uid)
		}
	}
}

// Тест: восстановление пустого кэша
func TestRestoreCache_Empty(t *testing.T) {
	// Подготовка
	repo := newMockRepo()
	cache := newMockCache()
	svc := NewOrderService(repo, cache)

	// Выполение - repo пустой
	err := svc.RestoreCache(context.Background())

	// Проверка
	if err != nil {
		t.Fatalf("RestoreCache failed: %v", err)
	}
	if cache.Size() != 0 {
		t.Errorf("Expected 0 orders in cache, got %d", cache.Size())
	}
}
