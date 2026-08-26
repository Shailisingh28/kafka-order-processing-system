# Event-Driven Order Processing System

An asynchronous order processing pipeline built with Apache Kafka, demonstrating
a fan-out event-driven architecture where a single event triggers multiple
independent consumers — with idempotency, retry, dead-letter handling, and
full observability built in.

## Why this project

Most backend systems eventually hit the same problem: a single request needs
to trigger several independent side effects (inventory check, notification,
analytics, etc.), and doing all of that synchronously makes the system slow
and fragile — if one step fails or is slow, the whole request suffers.

This project solves that with Kafka: the producer publishes an event and
returns immediately, while independent consumers process it in the
background, each at its own pace, without depending on each other.

## Architecture

```
        client ──POST /orders──▶ Spring Boot Producer ──▶ Kafka "orders" topic
                                                                  │
                    ┌─────────────────────────────────────────────┼─────────────────────────────┐
                    ▼                                             ▼                             ▼
         order-processing-group                          inventory-group                notification-group
         (Redis idempotency,                              (independent                   (independent
          retry + DLQ)                                     consumer group)                consumer group)
```

All three consumer groups subscribe to the same `orders` topic independently —
each gets its own copy of every event, and none of them know the others exist.

## Core concepts demonstrated

| Concept                                  | Where                                                                                                                                                                                                          |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Fan-out via consumer groups**          | Three independent consumer groups (`order-processing-group`, `inventory-group`, `notification-group`) all process the same event independently.                                                                |
| **At-least-once delivery & idempotency** | Kafka can redeliver messages after a crash before a commit completes. Redis-backed idempotency (tracking processed `orderId`s with a TTL) ensures duplicates are never double-processed, even after a restart. |
| **Retry + Dead Letter Queue**            | Failed messages are retried 3 times (2s backoff) via Spring Kafka's `DefaultErrorHandler`, then routed to `orders-dlq` — so one bad message never blocks the pipeline.                                         |
| **KRaft mode (no Zookeeper)**            | Migrated from a Zookeeper-based Kafka setup to KRaft mode, removing the external coordination dependency entirely.                                                                                             |

## Why Redis for idempotency (not in-memory)

An in-memory `Set` of processed order IDs would be lost on every restart,
silently reopening the door to duplicate processing. Storing processed IDs in
Redis (with a 24-hour TTL) means idempotency survives restarts — verified by
placing an order, restarting the app, and confirming a duplicate of that same
order was still correctly detected and skipped.

## Retry vs. Circuit Breaker (a deliberate distinction)

This project uses **retry + DLQ** rather than a circuit breaker, because the
failure mode here is different from a downstream HTTP call: a single bad
_message_ (not a systemically failing dependency) needs to be isolated
without stopping the consumer from processing the next message. Retry handles
transient failures; DLQ handles permanent ones.

## Metrics

Prometheus + Micrometer expose:

- `orders_processed_total`, `orders_duplicate_total`, `orders_failed_total`
- `orders_processing_time_seconds` (timing, including percentiles)
- `inventory_checks_total`, `notifications_sent_total`

**Note:** `orders_failed_total` increments per retry _attempt_, not per unique
failed order — a single permanently-failing message increments this counter
4 times (1 original attempt + 3 retries) before landing in the DLQ.

## Load test results

Ran using Gatling against the fully containerized system (producer + 3
consumers + Kafka + Redis, all in Docker Compose), ramping 5→50 requests/sec
over 30s, then holding at 50 req/sec for 30s.

| Metric                                 | Value         |
| -------------------------------------- | ------------- |
| Total requests                         | 2,325         |
| Failed requests (KO)                   | 0             |
| Mean throughput                        | 38.75 req/sec |
| p50 latency                            | 3ms           |
| p95 latency                            | 7ms           |
| p99 latency                            | 9ms           |
| Max latency                            | 261ms         |
| Consumer lag (all 3 groups, post-load) | 0             |

### Why latency is so much lower than the rate limiter project

The `/orders` endpoint only publishes an event and returns — it never waits
for downstream processing (inventory, notification, etc.) to complete. This
is the direct benefit of asynchronous, event-driven design: client-facing
latency is decoupled from background processing time. (Compare this to the
rate limiter project, where the gateway _did_ wait on a downstream HTTP call,
giving it a p99 of 177ms instead of 9ms here.)

### What I'd do differently at 10x scale

1. **Multiple partitions.** The `orders` topic currently has a single
   partition, meaning each consumer group can only use one consumer thread
   for true parallelism. At higher throughput, I'd increase partition count
   so multiple consumer instances per group could process in parallel.
2. **Separate DLQ per consumer group.** All three consumers currently share
   one `orders-dlq` topic. In production I'd give each consumer group its
   own DLQ (e.g. `inventory-group-dlq`) so failures are traceable to the
   exact service that produced them.
3. **Split into separately deployable services.** All three consumers
   currently run inside one Spring Boot app for simplicity. In production
   these would be independent deployable services, so one team's
   inventory-service bug can't affect the notification service's uptime.

## Running locally

```bash
docker compose up --build
```

Starts: Kafka (KRaft mode, no Zookeeper), Redis, and the Spring Boot app
(producer + all 3 consumers), fully containerized.

Create the topics (first run only):

```bash
docker exec -it kafka-order-system-kafka-1 kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka-order-system-kafka-1 kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Place an order:

```bash
curl -X POST "http://localhost:8080/orders?item=iPhone"
```

Check consumer lag:

```bash
docker exec -it kafka-order-system-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group order-processing-group
```

## Tech stack

Java 21, Spring Boot 4 (Spring Web + Spring Kafka), Apache Kafka (KRaft mode),
Redis, Resilience/Spring Retry, Docker Compose, Prometheus/Micrometer/Actuator,
Gatling for load testing.
