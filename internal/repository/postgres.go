package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"wb-order-service/internal/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("repository")

// PostgresRepository - хранилище, работающее с PostgreSQL
type PostgresRepository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// NewPostgresRepository создает новое подключение к БД
func NewPostgresRepository(ctx context.Context, connString string, timeout time.Duration) (*PostgresRepository, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	//Проверяем, что соединение действительно работает
	err = pool.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &PostgresRepository{pool: pool, timeout: timeout}, nil
}

// Close закрывает пул соединений
func (r *PostgresRepository) Close() {
	r.pool.Close()
}

// writeErr оборачивает ошибку записи
// Коды PostgreSQL группируются по первым двум символам — классу:
//   - 22 (data exception) — значение не подходит под тип: строка длиннее varchar(N), переполнение числа;
//   - 23 (integrity constraint violation) — нарушено ограничение: NOT NULL, CHECK, внешний ключ.
func writeErr(op string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) == 5 {
		if class := pgErr.Code[:2]; class == "22" || class == "23" {
			// Два %w (Go 1.20+): errors.Is найдёт и ErrInvalidOrder, и исходную ошибку pgx
			return fmt.Errorf("%s: %w: %w", op, models.ErrInvalidOrder, err)
		}
	}
	return fmt.Errorf("%s: %w", op, err)
}

// withTimeout оборачивает контекст таймаутом
func (r *PostgresRepository) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, r.timeout)
}

// SaveOrder сохраняет заказ в БД
func (r *PostgresRepository) SaveOrder(ctx context.Context, order models.Order) error {
	ctx, span := tracer.Start(ctx, "postgres.SaveOrder")
	defer span.End()

	ctx, cancel := r.withTimeout(ctx)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Если произойдет ошибка - откатим все изменения (после Commit это no-op)
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Все вставки отправляем ОДНИМ пакетом (pgx.Batch): pgx пишет запросы в соединение подряд,
	// не дожидаясь ответа на каждый (pipelining), и читает ответы потом.
	batch := &pgx.Batch{}

	// 1. orders
	batch.Queue(`
		INSERT INTO orders (
			order_uid, track_number, entry, locale,
			internal_signature, customer_id, delivery_service,
			shardkey, sm_id, date_created, oof_shard
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		order.OrderUID, order.TrackNumber, order.Entry, order.Locale,
		order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.ShardKey, order.SmID, order.DateCreated, order.OofShard,
	)

	// 2. deliveries
	batch.Queue(`
		INSERT INTO deliveries (
			order_uid, name, phone, zip, city, address, region, email
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		order.OrderUID,
		order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region,
		order.Delivery.Email,
	)

	// 3. payments
	batch.Queue(`
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

	// 4. items — по запросу на товар, но все в том же пакете
	for _, item := range order.Items {
		batch.Queue(`
			INSERT INTO items (
				order_uid, chrt_id, track_number, price, rid, name,
				sale, size, total_price, nm_id, brand, status
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			order.OrderUID,
			item.ChrtID, item.TrackNumber, item.Price, item.RID,
			item.Name, item.Sale, item.Size, item.TotalPrice,
			item.NmID, item.Brand, item.Status,
		)
	}

	if err := execBatch(ctx, tx, batch); err != nil {
		return err
	}

	// 5. Фиксируем транзакцию — все вставки применяются РАЗОМ
	if err := tx.Commit(ctx); err != nil {
		return writeErr("commit transaction", err)
	}
	return nil
}

// execBatch отправляет пакет и читает результаты строго по порядку постановки в очередь.
func execBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) error {
	br := tx.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			// 23505 — unique_violation: заказ с таким order_uid уже есть
			var pgErr *pgconn.PgError
			if i == 0 && errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return models.ErrOrderExists
			}
			return writeErr(batchStepName(i), err)
		}
	}
	return nil
}

// batchStepName — название шага пакета для текста ошибки
func batchStepName(i int) string {
	switch i {
	case 0:
		return "insert order"
	case 1:
		return "insert delivery"
	case 2:
		return "insert payment"
	default:
		return fmt.Sprintf("insert item %d", i-3)
	}
}

// GetOrder загружает заказ из БД по order_uid
func (r *PostgresRepository) GetOrder(ctx context.Context, orderUID string) (models.Order, error) {
	ctx, span := tracer.Start(ctx, "postgres.GetOrder")
	defer span.End()

	ctx, cancel := r.withTimeout(ctx)
	defer cancel()

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
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Order{}, models.ErrOrderNotFound
		}
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
	if err := rows.Err(); err != nil {
		return models.Order{}, fmt.Errorf("iterate items: %w", err)
	}

	return order, nil
}

// GetRecentOrders загружает последние limit заказов (по date_created) для прогрева кэша при старте.
func (r *PostgresRepository) GetRecentOrders(ctx context.Context, limit int) ([]models.Order, error) {
	ctx, span := tracer.Start(ctx, "postgres.GetRecentOrders")
	defer span.End()

	ctx, cancel := r.withTimeout(ctx)
	defer cancel()

	rows, err := r.pool.Query(ctx, `
		SELECT o.order_uid, o.track_number, o.entry, o.locale,
			o.internal_signature, o.customer_id, o.delivery_service,
			o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider,
			p.amount, p.payment_dt, p.bank, p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		JOIN deliveries d ON d.order_uid = o.order_uid
		JOIN payments p ON p.order_uid = o.order_uid
		ORDER BY o.date_created DESC
		LIMIT $1`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select recent orders: %w", err)
	}
	defer rows.Close()

	orders := make([]models.Order, 0, limit)
	for rows.Next() {
		var o models.Order
		err = rows.Scan(
			&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale,
			&o.InternalSignature, &o.CustomerID, &o.DeliveryService,
			&o.ShardKey, &o.SmID, &o.DateCreated, &o.OofShard,
			&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip,
			&o.Delivery.City, &o.Delivery.Address, &o.Delivery.Region,
			&o.Delivery.Email,
			&o.Payment.Transaction, &o.Payment.RequestID,
			&o.Payment.Currency, &o.Payment.Provider,
			&o.Payment.Amount, &o.Payment.PaymentDT,
			&o.Payment.Bank, &o.Payment.DeliveryCost,
			&o.Payment.GoodsTotal, &o.Payment.CustomFee,
		)
		if err != nil {
			return nil, fmt.Errorf("scan recent order: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent orders: %w", err)
	}
	if len(orders) == 0 {
		return orders, nil
	}

	uids := make([]string, len(orders))
	idx := make(map[string]int, len(orders)) // order_uid -> позиция в orders
	for i, o := range orders {
		uids[i] = o.OrderUID
		idx[o.OrderUID] = i
	}

	itemRows, err := r.pool.Query(ctx, `
		SELECT order_uid, chrt_id, track_number, price, rid, name,
			sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid = ANY($1)
		ORDER BY id`, uids,
	)
	if err != nil {
		return nil, fmt.Errorf("select items of recent orders: %w", err)
	}
	defer itemRows.Close()

	for itemRows.Next() {
		var uid string
		var item models.Item
		err = itemRows.Scan(
			&uid, &item.ChrtID, &item.TrackNumber, &item.Price, &item.RID,
			&item.Name, &item.Sale, &item.Size, &item.TotalPrice,
			&item.NmID, &item.Brand, &item.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("scan item: %w", err)
		}
		i := idx[uid]
		orders[i].Items = append(orders[i].Items, item)
	}
	if err := itemRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate items: %w", err)
	}

	return orders, nil
}
