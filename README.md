# Order & Inventory: event-driven integration with Kafka

Two independent Spring Boot services that communicate through Apache Kafka:

- **order-api** accepts orders over REST and publishes an `OrderCreated` event.
- **inventory-service** consumes the events and reserves stock (in memory).

The business logic is deliberately simple. The focus is on the **reliability of the integration**: delivery guarantees,
idempotent processing, retries, dead-lettering and clear failure behavior.

```
                 POST /orders
                      │
              ┌───────▼────────┐
              │   order-api    │  validate → publish (wait for broker ack) → 202
              └───────┬────────┘
                      │  topic: orders.v1   (key = itemId, 3 partitions)
              ┌───────▼────────┐
              │     Kafka      │  single node, KRaft
              └───────┬────────┘
                      │
              ┌───────▼────────┐   invalid / failed after retries
              │   inventory-   │ ─────────────────────────────────► orders.v1.DLT
              │    service     │
              └───────┬────────┘
                      │
          GET /inventory, GET /reservations[/{orderId}]
```

**Stack:** Java 25, Spring Boot 4.1, Spring for Apache Kafka 4, Apache Kafka 4.4 (KRaft), Maven multi-module, JUnit 5,
Testcontainers, Docker Compose.

---

## Quick start

**Prerequisite:** Docker (with Docker Compose v2).

```bash
git clone https://github.com/zigy035/order-inventory-system.git
cd order-inventory-system
docker compose up -d --build
docker compose ps        # wait until order-api and inventory-service are (healthy)
```

| Component         | URL / address         |
|-------------------|-----------------------|
| order-api         | http://localhost:8080 |
| inventory-service | http://localhost:8081 |
| Kafka (from host) | localhost:9092        |

Stop and remove everything (including all Kafka data):

```bash
docker compose down
```

---

## Trying it out

Initial stock (configured in `inventory-service/src/main/resources/application.yaml`):

| itemId | stock | useful for                      |
|--------|-------|---------------------------------|
| item-1 | 10    | successful reservations         |
| item-2 | 5     | insufficient stock              |
| item-3 | 0     | item exists but is out of stock |
| other  | —     | unknown item                    |

### Place orders

```bash
# reserved
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"o-1","itemId":"item-1","quantity":3}'

# rejected: insufficient stock
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"o-2","itemId":"item-2","quantity":6}'

# rejected: unknown item
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"o-3","itemId":"item-99","quantity":1}'

# duplicate of o-1: accepted by the API, ignored by inventory (stock not reduced again)
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"o-1","itemId":"item-1","quantity":3}'

# validation error: 400 with field errors
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"","itemId":"item-1","quantity":0}'
```

`POST /orders` returns **202 Accepted** once the event is durably written to Kafka. Processing is asynchronous.

### Check results

```bash
curl -s localhost:8081/inventory            # {"item-1":7,"item-2":5,"item-3":0}
curl -s localhost:8081/reservations         # all processed orders, oldest first
curl -s localhost:8081/reservations/o-1     # outcome of one order (404 if not processed yet)
docker compose logs -f inventory-service    # processing log
```

Possible reservation statuses: `RESERVED`, `REJECTED_INSUFFICIENT_STOCK`, `REJECTED_UNKNOWN_ITEM`.

### See the failure handling

Simulate Kafka being unavailable; the API answers `503` with `Retry-After` after ~5 s instead of hanging:

```bash
docker compose stop kafka
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"o-4","itemId":"item-1","quantity":1}'
docker compose start kafka
```

Send malformed messages directly to Kafka (bypassing the API's validation):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic orders.v1 \
  --property parse.key=true --property key.separator='|'
```

```
item-1|this is not json
item-1|{"eventId":"11111111-1111-1111-1111-111111111111","orderId":"","itemId":"item-1","quantity":0}
```

Both go straight to the dead-letter topic without retries. Inspect it (original payload plus `kafka_dlt-*` headers with
the reason):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic orders.v1.DLT --from-beginning \
  --property print.key=true --property print.headers=true
```

---

## Running the tests

Tests need Docker running (integration tests start a real Kafka with Testcontainers).

```bash
./mvnw clean verify
```

| Test                       | What it covers                                                                                                                                           |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `InventoryServiceTest`     | reservation rules, boundaries, idempotency, concurrency (no overselling, single processing of the same order under parallel delivery)                    |
| `InventoryIntegrationTest` | valid event → reservation; malformed JSON → DLT with original bytes; transient failure → retried and processed; persistent failure → 4 attempts then DLT |
| `OrderApiIntegrationTest`  | `POST /orders` → 202 and an event on Kafka with key = `itemId` and the expected JSON; validation errors → 400                                            |

Tests are not run inside the Docker image build (`-DskipTests`): the image only packages code verified by
`./mvnw verify`, and Testcontainers cannot run inside `docker build`.

## Running from the IDE

```bash
docker compose up -d kafka kafka-init
```

Then run `OrderApiApplication` and `InventoryServiceApplication`. Without the `KAFKA_BOOTSTRAP_SERVERS` variable, both
connect to `localhost:9092`.

---

## Event contract

Topic `orders.v1`, key = `itemId`, value = JSON:

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "occurredAt": "2026-10-29T12:30:48.052Z",
  "orderId": "123",
  "itemId": "item-1",
  "quantity": 2
}
```

The services share **only this JSON contract**, not code: each has its own `OrderCreatedEvent` class. The consumer
ignores unknown fields (tolerant reader), so the producer can add fields without a coordinated release. An incompatible
change would be published to a new topic (`orders.v2`).

---

## Design decisions

### Delivery semantics: at-least-once + idempotent processing

Exactly-once end to end is expensive and complex. The system instead guarantees that **no accepted order is lost**
(at-least-once) and makes duplicates **harmless** (idempotent consumer). The result is effectively-once processing.

### order-api (producer)

- **202 only after the broker acknowledges the write.** The publisher waits for the send result; if it fails, the client
  gets **503 + `Retry-After`** and knows to retry. Fire-and-forget would be faster but could lose orders silently.
- **`acks=all` + `enable.idempotence=true`**: durable writes, and producer retries cannot create duplicates in the
  topic.
- **Bounded timeouts**: `max.block.ms=5000` (the default of 60 s would hang HTTP requests when Kafka is down),
  `delivery.timeout.ms=10000`.
- **Key = `itemId`**: all orders for the same item land in the same partition and are processed in order.
- **No type headers** (`spring.json.add.type.headers=false`): the message does not carry producer Java class names; the
  consumer decides the target type.
- **No transactional outbox**: order-api has no database, so there is no dual write (DB + broker) to keep consistent.
  The publish itself is the only state change. With a database, an outbox would be the right tool.

### inventory-service (consumer)

- **Idempotency by `orderId`** (business key, not `eventId`): a client retrying `POST` creates a new event with the same
  `orderId`. The first outcome is final ("first write wins").
- **Thread-safe domain**: `ConcurrentHashMap.computeIfAbsent` makes check-and-record atomic; stock is decremented with a
  lock-free compare-and-set loop. Partitioning by `itemId` already serialises work per item; the atomic operations make
  the domain correct regardless (defence in depth).
- **Offsets committed after processing** (`ack-mode: record`, auto-commit off), **`auto-offset-reset: earliest`** so
  orders published before the consumer first starts are not missed.
- **`concurrency: 3`**: one consumer thread per partition.
- **Business rejection is not an error**: insufficient stock or unknown item is a valid outcome, recorded and committed.
  Only messages the service cannot process go to the DLT.

### Error handling

| Failure                                    | Handling                                                                                     |
|--------------------------------------------|----------------------------------------------------------------------------------------------|
| Malformed JSON                             | `ErrorHandlingDeserializer` → DLT immediately, original bytes preserved (no stuck partition) |
| Invalid content (missing / invalid fields) | `InvalidOrderEventException` (non-retryable) → DLT immediately                               |
| Any other exception (possibly transient)   | blocking retry, 4 attempts in total, exponential backoff 500 ms → 1 s → 2 s, then DLT        |
| DLT publish fails                          | offset not committed, record is redelivered                                                  |

- **Blocking retry instead of retry topics (`@RetryableTopic`)**: retry topics keep the partition moving but break
  per-item ordering, which the partitioning strategy exists to preserve. With a short total backoff (~3.5 s), blocking
  is the better trade-off here.
- The DLT has the same number of partitions as the source topic; records keep their original partition.
- Every failed attempt, every record sent to the DLT and every failed DLT publish is logged with topic, partition,
  offset and root cause.

### Infrastructure

- Topics are created explicitly by the `kafka-init` container; broker auto-creation is disabled so a typo in a topic
  name cannot silently create a new topic.
- Services start only after the topics exist; at runtime they recover on their own if Kafka becomes unavailable.
- Multi-stage Docker images, non-root user, JVM heap sized relative to the container memory limit, health checks on
  `/actuator/health/readiness`.

---

## Assumptions

- `orderId` is unique per order and immutable; a repeated `orderId` is treated as a duplicate regardless of its content.
- Stock is a fixed set of items configured at startup; there is no restocking.
- The client learns the outcome by polling `GET /reservations/{orderId}` (eventual consistency; a `404` right after
  `POST` can mean "not processed yet").

## Known limitations and what I would do in production

- **In-memory state.** Stock and processed orders are lost when inventory-service restarts, while Kafka keeps the
  committed offsets, so already processed orders are not replayed and stock starts from the initial values again. In
  production both would live in a database, with the idempotency check and the stock update in one transaction.
- **Single broker, replication factor 1.** `acks=all` gives no extra durability with one replica. Production: 3 brokers,
  replication factor 3, `min.insync.replicas=2`.
- **DLT content for invalid-content failures** is the re-serialized event (equivalent JSON, e.g. a missing `occurredAt`
  appears as `null`), not the original bytes. Malformed JSON is stored byte for byte.
- **No outcome event.** Inventory could publish `ReservationCompleted` / `ReservationRejected` to a result topic for
  order-api or other services instead of clients polling.
- **Unbounded reservation map**; production would use TTL / cleanup of old idempotency records.
- No authentication, metrics dashboard or distributed tracing.

---

## Project structure

```
order-inventory-system/
├── pom.xml                       parent: Java version, module list
├── docker-compose.yml            Kafka, topic init, both services
├── order-api/
│   ├── Dockerfile
│   └── src/main/java/rs/mds/order/
│       ├── api/                  controller, request/response, error handling (ProblemDetail)
│       ├── service/              OrderService
│       ├── messaging/            event, publisher
│       └── config/               typed configuration properties
└── inventory-service/
    ├── Dockerfile
    └── src/main/java/rs/mds/inventory/
        ├── domain/               framework-free reservation logic
        ├── messaging/            event, listener, validation
        ├── config/               properties, beans, error handler + DLT
        └── api/                  read-only endpoints
```
