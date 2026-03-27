-- Таблица заказов (основная)
CREATE TABLE IF NOT EXISTS orders (
    order_uid         VARCHAR(255) PRIMARY KEY, 
    track_number      VARCHAR(255) NOT NULL,
    entry             VARCHAR(50)  NOT NULL,
    locale            VARCHAR(10),
    internal_signature VARCHAR(255),
    customer_id       VARCHAR(255) NOT NULL,
    delivery_service  VARCHAR(100),
    shardkey          VARCHAR(10),
    sm_id             INTEGER,
    date_created      TIMESTAMPTZ  NOT NULL,
    oof_shard         VARCHAR(10)
);

-- Таблица доставки (1:1 с orders)
CREATE TABLE IF NOT EXISTS deliveries (
    id        SERIAL PRIMARY KEY,
    order_uid VARCHAR(255) NOT NULL UNIQUE REFERENCES orders(order_uid) ON DELETE CASCADE,
    name      VARCHAR(255) NOT NULL,
    phone     VARCHAR(50),
    zip       VARCHAR(20),
    city      VARCHAR(100),
    address   VARCHAR(255),
    region    VARCHAR(100),
    email     VARCHAR(100)
);

-- Таблица оплаты (1:1 с orders)
CREATE TABLE IF NOT EXISTS payments (
    id            SERIAL PRIMARY KEY,
    order_uid     VARCHAR(255) NOT NULL UNIQUE REFERENCES orders(order_uid) ON DELETE CASCADE,
    transaction   VARCHAR(255) NOT NULL,
    request_id    VARCHAR(255),
    currency      VARCHAR(10),
    provider      VARCHAR(50),
    amount        INTEGER,
    payment_dt    BIGINT,
    bank          VARCHAR(50),
    delivery_cost INTEGER,
    goods_total   INTEGER,
    custom_fee    INTEGER
);

-- Таблица товаров (1:N с orders)
CREATE TABLE IF NOT EXISTS items (
    id           SERIAL PRIMARY KEY,
    order_uid    VARCHAR(255) NOT NULL REFERENCES orders(order_uid) ON DELETE CASCADE,
    chrt_id      BIGINT       NOT NULL,
    track_number VARCHAR(255),
    price        INTEGER,
    rid          VARCHAR(255),
    name         VARCHAR(255),
    sale         INTEGER,
    size         VARCHAR(50),
    total_price  INTEGER,
    nm_id        BIGINT,
    brand        VARCHAR(255),
    status       INTEGER
);

CREATE INDEX IF NOT EXISTS idx_items_order_uid ON items(order_uid);