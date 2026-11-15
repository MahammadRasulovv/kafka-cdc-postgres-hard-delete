import json
import logging
import os

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092")
TOPIC = os.getenv("CDC_TOPIC", "cdc.public.orders")
GROUP_ID = os.getenv("GROUP_ID", "cdc-event-log")

LABELS = {"r": "SNAPSHOT", "c": "INSERT", "u": "UPDATE", "d": "DELETE"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cdc-event-log")

def describe(event: dict) -> str:
    op = event.get("op")
    before = event.get("before") or {}
    after = event.get("after") or {}
    label = LABELS.get(op, f"UNKNOWN({op})")

    if op in ("r", "c"):
        return f"{label} id={after.get('id')} {after}"
    if op == "u":
        changes = {k: f"{before.get(k)} -> {v}" for k, v in after.items() if before.get(k) != v}
        return f"{label} id={after.get('id')} changes={changes}"
    if op == "d":
        return f"{label} id={before.get('id')} before={before}"
    return f"{label} {event}"

def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    log.info("CDC event log started: topic=%s group=%s", TOPIC, GROUP_ID)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue
            key = msg.key().decode() if msg.key() else None
            if msg.value() is None:
                log.info("%s/%s TOMBSTONE key=%s", msg.partition(), msg.offset(), key)
            else:
                try:
                    event = json.loads(msg.value())
                    log.info("%s/%s %s", msg.partition(), msg.offset(), describe(event))
                except (ValueError, TypeError) as exc:
                    log.error("%s/%s cannot parse event: %s", msg.partition(), msg.offset(), exc)
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
