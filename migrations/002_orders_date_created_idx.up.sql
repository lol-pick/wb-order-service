-- Индекс для прогрева кэша: ORDER BY date_created DESC LIMIT N
-- читает N первых записей индекса вместо сортировки всей таблицы
CREATE INDEX IF NOT EXISTS idx_orders_date_created ON orders(date_created);
