CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    item        VARCHAR(100),
    amount      NUMERIC(10,2),
    status      VARCHAR(30) DEFAULT 'pending',
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders (customer_id, item, amount, status) VALUES
    ('cust-001', 'Laptop',   1500.00, 'pending'),
    ('cust-002', 'Monitor',   450.00, 'pending'),
    ('cust-001', 'Keyboard',   85.00, 'completed');


SHOW wal_level;


SELECT * FROM orders;

delete from orders ;


INSERT INTO orders (customer_id, item, amount, status) VALUES ('cust-010', 'Tablet', 299.00, 'pending');

UPDATE orders SET status = 'shipped', updated_at = NOW() WHERE id = 7;

DELETE FROM orders WHERE id = 8;
