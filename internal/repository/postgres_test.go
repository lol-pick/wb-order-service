package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"wb-order-service/internal/models"
)

// getTestConnString - строка подключения для тестов
func getTestConnString() string {
	conn := os.Getenv("TEST_POSTGRES_CONN")
	if conn == "" {
		conn = os.Getenv("POSTGRES_USER")
		if conn == "" {
			// Собираем из отдельных переменных (как в config.go)
			host := getEnvOrDefault("POSTGRES_HOST", "localhost")
			port := getEnvOrDefault("POSTGRES_PORT", "5432")
			user := getEnvOrDefault("POSTGRES_USER", "wb_user")
			pass := getEnvOrDefault("POSTGRES_PASSWORD", "wb_password")
			db := getEnvOrDefault("POSTGRES_DB", "wb_orders")
			return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, db)
		}
	}
	return conn
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// createIntegrationTestOrder - тестовый заказ с уникальным ID
func createIntegrationTestOrder() models.Order {
	uid := fmt.Sprintf("inttest-%d", time.Now().UnixNano())
	return models.Order{
		OrderUID:        uid,
		TrackNumber:     "TESTTRACK",
		Entry:           "TEST",
		CustomerID:      "test-customer",
		DeliveryService: "test-delivery",
		Locale:          "en",
		ShardKey:        "1",
		SmID:            1,
		DateCreated:     time.Now().UTC().Truncate(time.Microsecond),
		OofShard:        "1",
		Delivery: models.Delivery{
			Name:    "Integration Test User",
			Phone:   "+71234567890",
			Zip:     "123456",
			City:    "Moscow",
			Address: "Test Street 1",
			Region:  "Central",
			Email:   "inttest@test.com",
		},
		Payment: models.Payment{
			Transaction:  uid,
			Currency:     "RUB",
			Provider:     "test",
			Amount:       1500,
			PaymentDT:    time.Now().Unix(),
			Bank:         "test-bank",
			DeliveryCost: 500,
			GoodsTotal:   1000,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      11111,
				TrackNumber: "TESTTRACK",
				Price:       500,
				RID:         "rid-001",
				Name:        "Sneakers",
				Sale:        10,
				Size:        "42",
				TotalPrice:  450,
				NmID:        67890,
				Brand:       "Nike",
				Status:      202,
			},
			{
				ChrtID:      22222,
				TrackNumber: "TESTTRACK",
				Price:       300,
				RID:         "rid-002",
				Name:        "T-Shirt",
				Sale:        20,
				Size:        "L",
				TotalPrice:  240,
				NmID:        12345,
				Brand:       "Adidas",
				Status:      202,
			},
		},
	}
}

// Тест подключение к БД
func TestPostgres_Connection(t *testing.T) {
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, getTestConnString(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to DB: %v", err)
	}
	defer repo.Close()
}

// Тест сохранине и поолучение заказа из бд
func TestPostgres_SaveAndGetOrder(t *testing.T) {
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, getTestConnString(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer repo.Close()

	order := createIntegrationTestOrder()

	// Сохраняем
	err = repo.SaveOrder(ctx, order)
	if err != nil {
		t.Fatalf("SaveOrder failed: %v", err)
	}

	// Получаем
	got, err := repo.GetOrder(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("GetOrder failed: %v", err)
	}

	// Проверяем основные поля
	if got.OrderUID != order.OrderUID {
		t.Errorf("OrderUID: expected %s, got %s", order.OrderUID, got.OrderUID)
	}
	if got.TrackNumber != order.TrackNumber {
		t.Errorf("TrackNumber: expected %s, got %s", order.TrackNumber, got.TrackNumber)
	}
	if got.CustomerID != order.CustomerID {
		t.Errorf("CustomerID: expected %s, got %s", order.CustomerID, got.CustomerID)
	}

	// Проверяем доставку
	if got.Delivery.Name != order.Delivery.Name {
		t.Errorf("Delivery.Name: expected %s, got %s", order.Delivery.Name, got.Delivery.Name)
	}
	if got.Delivery.City != order.Delivery.City {
		t.Errorf("Delivery.City: expected %s, got %s", order.Delivery.City, got.Delivery.City)
	}

	// Проверяем оплату
	if got.Payment.Amount != order.Payment.Amount {
		t.Errorf("Payment.Amount: expected %d, got %d", order.Payment.Amount, got.Payment.Amount)
	}
	if got.Payment.Currency != order.Payment.Currency {
		t.Errorf("Payment.Currency: expected %s, got %s", order.Payment.Currency, got.Payment.Currency)
	}

	// Проверяем товары
	if len(got.Items) != len(order.Items) {
		t.Fatalf("Items count: expected %d, got %d", len(order.Items), len(got.Items))
	}

	for i := range order.Items {
		expected := order.Items[i]
		actual := got.Items[i]
		if actual.Name != expected.Name {
			t.Errorf("Item[%d].Name: expected %s, got %s", i, expected.Name, actual.Name)
		}
		if actual.Brand != expected.Brand {
			t.Errorf("Item[%d].Brand: expected %s, got %s", i, expected.Brand, actual.Brand)
		}
	}
}

// Тест дубликат в бд
func TestPostgres_DuplicateOrder(t *testing.T) {
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, getTestConnString(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer repo.Close()

	order := createIntegrationTestOrder()

	// Первое сохрание - ОК
	err = repo.SaveOrder(ctx, order)
	if err != nil {
		t.Fatalf("First save failed: %v", err)
	}

	// Второе сохрание - должно быть ошибка дубликата
	err = repo.SaveOrder(ctx, order)
	if !errors.Is(err, models.ErrOrderExists) {
		t.Fatalf("Expected ErrOrderExists, got %v", err)
	}
}

// Тест: получение несуществующего заказа
func TestPostgres_GetOrder_NotFound(t *testing.T) {
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, getTestConnString(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer repo.Close()

	_, err = repo.GetOrder(ctx, "nonexistent-order-id")
	if !errors.Is(err, models.ErrOrderNotFound) {
		t.Fatalf("Expected ErrOrderNotFound, got %v", err)
	}
}

// Тест GetRecentOrders: возвращает самые свежие заказы, не больше limit, вместе с товарами
func TestPostgres_GetRecentOrders(t *testing.T) {
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, getTestConnString(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer repo.Close()

	// Три заказа «из будущего», которые должны оказаться новее всего в тестовой БД.
	// Тестовая БД не очищается между запусками, поэтому заказы прошлых прогонов остаются.
	// Шаг между нашими заказами — 1 мс, а не часы: тогда заказы прошлого прогона
	// (у них меньше base) всегда старше заказов текущего, даже если прогоны идут подряд.
	// Раньше шаг был 1 час: заказ «+3ч» прогона минутной давности оказывался новее нашего «+2ч».
	base := time.Now().UTC().Add(1000 * time.Hour).Truncate(time.Microsecond)
	var saved []models.Order
	for i := 1; i <= 3; i++ {
		order := createIntegrationTestOrder()
		order.DateCreated = base.Add(time.Duration(i) * time.Millisecond)
		if err := repo.SaveOrder(ctx, order); err != nil {
			t.Fatalf("SaveOrder failed: %v", err)
		}
		saved = append(saved, order)
	}

	orders, err := repo.GetRecentOrders(ctx, 2)
	if err != nil {
		t.Fatalf("GetRecentOrders failed: %v", err)
	}
	if len(orders) != 2 {
		t.Fatalf("Expected 2 orders, got %d", len(orders))
	}

	// Порядок: сначала самый новый (+3 часа), потом +2 часа
	if orders[0].OrderUID != saved[2].OrderUID || orders[1].OrderUID != saved[1].OrderUID {
		t.Errorf("Unexpected order: got %s, %s", orders[0].OrderUID, orders[1].OrderUID)
	}
	// Товары подтянулись вторым запросом
	if len(orders[0].Items) != len(saved[2].Items) {
		t.Errorf("Expected %d items, got %d", len(saved[2].Items), len(orders[0].Items))
	}
	if orders[0].Delivery.Name != saved[2].Delivery.Name {
		t.Errorf("Delivery not loaded")
	}
}
