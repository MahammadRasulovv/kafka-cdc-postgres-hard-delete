import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092")
TOPIC = os.getenv("CDC_TOPIC", "cdc.public.orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "cdc.public.orders.dlq")
GROUP_ID = os.getenv("GROUP_ID", "cdc-db-sync")
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5433")),
    "dbname": os.getenv("PG_DB", "targetdb"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
    "connect_timeout": 10,
}

UPSERT_SQL = """
    INSERT INTO orders
        (id, customer_id, item, amount, status, created_at, updated_at, cdc_operation, synced_at)
    VALUES
        (%(id)s, %(customer_id)s, %(item)s, %(amount)s, %(status)s,
         %(created_at)s, %(updated_at)s, %(op)s, NOW())
    ON CONFLICT (id) DO UPDATE SET
        customer_id    = EXCLUDED.customer_id,
        item           = EXCLUDED.item,
        amount         = EXCLUDED.amount,
        status         = EXCLUDED.status,
        created_at     = EXCLUDED.created_at,
        updated_at     = EXCLUDED.updated_at,
        cdc_operation  = EXCLUDED.cdc_operation,
        synced_at      = NOW()
"""

# HARD DELETE: the row is physically removed from the target table
HARD_DELETE_SQL = """
    DELETE FROM orders
    WHERE id = %(id)s
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cdc-db-sync")


def to_timestamp(micros):
    """Debezium sends TIMESTAMP (without time zone) as microseconds since epoch."""
    if micros is None:
        return None
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(tzinfo=None)


def to_amount(value):
    if value is None:
        return None
    return Decimal(value)


def row_params(row: dict, op_name: str) -> dict:
    return {
        "id": row["id"],
        "customer_id": row.get("customer_id"),
        "item": row.get("item"),
        "amount": to_amount(row.get("amount")),
        "status": row.get("status"),
        "created_at": to_timestamp(row.get("created_at")),
        "updated_at": to_timestamp(row.get("updated_at")),
        "op": op_name,
    }


def apply_event(cur, event: dict) -> str:
    """Apply one Debezium event to the target table; return a log line."""
    op = event.get("op")
    before = event.get("before") or {}
    after = event.get("after") or {}

    if op in ("r", "c"):
        cur.execute(UPSERT_SQL, row_params(after, "INSERT"))
        return f"{'SNAPSHOT' if op == 'r' else 'INSERT'}: id={after['id']}"
    if op == "u":
        cur.execute(UPSERT_SQL, row_params(after, "UPDATE"))
        return f"UPDATE id={after['id']} status: {before.get('status')} -> {after.get('status')}"
    if op == "d":
        cur.execute(HARD_DELETE_SQL, {"id": before["id"]})
        return f"DELETE id={before['id']} -> hard deleted (rows affected: {cur.rowcount})"
    raise ValueError(f"unknown op: {op}")


def send_to_dlq(producer: Producer, msg, reason: str) -> bool:
    """Send a failed message to the DLQ topic. Returns True only if delivery succeeded."""
    errors: list[str] = []
    producer.produce(
        DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[
            ("error", reason.encode()),
            ("source", f"{msg.topic()}/{msg.partition()}/{msg.offset()}".encode()),
        ],
        on_delivery=lambda err, _m: errors.append(str(err)) if err else None,
    )
    if producer.flush(10) > 0 or errors:
        log.error("Failed to send message to DLQ: %s", errors)
        return False
    return True


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    consumer.subscribe([TOPIC])
    log.info("CDC DB sync started: %s -> %s:%s/%s", TOPIC, PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"])

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            try:
                if msg.value() is None:  # tombstone after a delete: nothing to apply
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                event = json.loads(msg.value())
                with conn.cursor() as cur:
                    summary = apply_event(cur, event)
                conn.commit()
                log.info("%s/%s %s", msg.partition(), msg.offset(), summary)
                consumer.commit(message=msg, asynchronous=False)
            except Exception as exc:
                conn.rollback()
                log.error("%s/%s error processing event: %s", msg.partition(), msg.offset(), exc)
                if send_to_dlq(producer, msg, str(exc)):
                    consumer.commit(message=msg, asynchronous=False)
                else:
                    # do not commit: the message must not be lost
                    log.critical("DLQ unavailable, stopping so the offset is not lost")
                    break
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
