package repository

import (
	"context"
	"fmt"
	"wb-order-service/internal/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository - хранилище, работающее с PostgreSQL
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, connString string) (*PostgresRepository, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	//Проверяем, что соединение действительно работает
	err = pool.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &PostgresRepository{pool: pool}, nil
}

// Close закрывает пул соединений
func (r *PostgresRepository) Close() {
	r.pool.Close()
}

// SaveOrder сохраняет заказ в БД (в транзакции)
func (r *PostgresRepository) SaveOrder(ctx context.Context, order models.Order) error {
	//Начинаем транзакцию
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Если произойдет ошибка - откатим все изменения
	defer tx.Rollback(ctx)

	// 1. Вставляем таблицу orders
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (
			order_uid, track_number, entry, locale,
			internal_signature, customer_id, delivery_service,
			shardkey, sm_id, date_created, oof_shard
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		order.OrderUID, order.TrackNumber, order.Entry, order.Locale,
		order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.ShardKey, order.SmID, order.DateCreated, order.OofShard,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	// 2. Вставляем в таблицу deliveries
	_, err = tx.Exec(ctx, `
		INSERT INTO deliveries (
			order_uid, name, phone, zip, city, address, region, email
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		order.OrderUID,
		order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region,
		order.Delivery.Email,
	)
	if err != nil {
		return fmt.Errorf("insert delivery: %w", err)
	}

	// 3. Вставляем в таблицу payments
	_, err = tx.Exec(ctx, `
		INSERT INTO payments (
			order_uid, transaction, request_id, currency, provider,
			amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		order.OrderUID,
		order.Payment.Transaction, order.Payment.RequestID,
		order.Payment.Currency, order.Payment.Provider,
		order.Payment.Amount, order.Payment.PaymentDT,
		order.Payment.Bank, order.Payment.DeliveryCost,
		order.Payment.GoodsTotal, order.Payment.CustomFee,
	)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}

	// 4. Вставляем каждый товар в таблицу items
	for i, item := range order.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO items (
				order_uid, chrt_id, track_number, price, rid, name,
				sale, size, total_price, nm_id, brand, status
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			order.OrderUID,
			item.ChrtID, item.TrackNumber, item.Price, item.RID,
			item.Name, item.Sale, item.Size, item.TotalPrice,
			item.NmID, item.Brand, item.Status,
		)
		if err != nil {
			return fmt.Errorf("insert item %d: %w", i, err)
		}
	}

	// 5. Фиксируем транзакцию — все 4 вставки применяются РАЗОМ
	return tx.Commit(ctx)
}

// GetOrder загружает заказ из БД по order_uid
func (r *PostgresRepository) GetOrder(ctx context.Context, orderUID string) (models.Order, error) {
	var order models.Order

	// 1. Получаем данные из таблицы orders
	err := r.pool.QueryRow(ctx, `
		SELECT order_uid, track_number, entry, locale,
			internal_signature, customer_id, delivery_service,
			shardkey, sm_id, date_created, oof_shard
		FROM orders WHERE order_uid = $1`, orderUID,
	).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale,
		&order.InternalSignature, &order.CustomerID, &order.DeliveryService,
		&order.ShardKey, &order.SmID, &order.DateCreated, &order.OofShard,
	)
	if err != nil {
		return models.Order{}, fmt.Errorf("select order: %w", err)
	}

	// 2. Получаем данные доставки
	err = r.pool.QueryRow(ctx, `
		SELECT name, phone, zip, city, address, region, email
		FROM deliveries WHERE order_uid = $1`, orderUID,
	).Scan(
		&order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip,
		&order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region,
		&order.Delivery.Email,
	)
	if err != nil {
		return models.Order{}, fmt.Errorf("select delivery: %w", err)
	}

	// 3. Получаем данные оплаты
	err = r.pool.QueryRow(ctx, `
		SELECT transaction, request_id, currency, provider,
			amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
		FROM payments WHERE order_uid = $1`, orderUID,
	).Scan(
		&order.Payment.Transaction, &order.Payment.RequestID,
		&order.Payment.Currency, &order.Payment.Provider,
		&order.Payment.Amount, &order.Payment.PaymentDT,
		&order.Payment.Bank, &order.Payment.DeliveryCost,
		&order.Payment.GoodsTotal, &order.Payment.CustomFee,
	)
	if err != nil {
		return models.Order{}, fmt.Errorf("select payment: %w", err)
	}

	// 4. Получаем все товары заказа
	rows, err := r.pool.Query(ctx, `
		SELECT chrt_id, track_number, price, rid, name,
			sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid = $1`, orderUID,
	)
	if err != nil {
		return models.Order{}, fmt.Errorf("select items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item models.Item
		err = rows.Scan(
			&item.ChrtID, &item.TrackNumber, &item.Price, &item.RID,
			&item.Name, &item.Sale, &item.Size, &item.TotalPrice,
			&item.NmID, &item.Brand, &item.Status,
		)
		if err != nil {
			return models.Order{}, fmt.Errorf("scan item: %w", err)
		}
		order.Items = append(order.Items, item)
	}

	return order, nil
}

// GetAllOrders загружает все заказы из БД (для восстановления кэша при старте)
func (r *PostgresRepository) GetAllOrders(ctx context.Context) ([]models.Order, error) {
	// 1. Получаем все order_uid
	rows, err := r.pool.Query(ctx, "SELECT order_uid FROM orders")
	if err != nil {
		return nil, fmt.Errorf("select all order uids: %w", err)
	}
	defer rows.Close()

	var orderUIDs []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("scan order uid: %w", err)
		}
		orderUIDs = append(orderUIDs, uid)
	}

	// 2. Для каждого order_uid загружаем полный заказ
	var orders []models.Order
	for _, uid := range orderUIDs {
		order, err := r.GetOrder(ctx, uid)
		if err != nil {
			return nil, fmt.Errorf("get order %s: %w", uid, err)
		}
		orders = append(orders, order)
	}

	return orders, nil
}
