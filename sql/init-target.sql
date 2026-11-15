CREATE TABLE IF NOT EXISTS orders (
    id            INTEGER PRIMARY KEY,
    customer_id   VARCHAR(50),
    item          VARCHAR(100),
    amount        NUMERIC(10,2),
    status        VARCHAR(30),
    created_at    TIMESTAMP,
    updated_at    TIMESTAMP,
    cdc_operation VARCHAR(20),
    synced_at     TIMESTAMP
);
