# Message Queue PoC for a Vietnamese E-commerce Microservices System

This project is a lightweight but realistic proof-of-concept designed to compare synchronous REST orchestration against asynchronous broker-driven processing in a microservices architecture.

## Included services

- API Gateway
- Order Service
- Payment Service
- Inventory Service
- Notification Service
- Analytics Service

## Goals

1. Show the latency amplification of synchronous orchestration.
2. Demonstrate the benefits of event-driven processing with RabbitMQ.
3. Capture failure and retry scenarios.
4. Keep the architecture broker-agnostic so later ActiveMQ and Kafka adapters can be added without rewriting business logic.

## Stack

- Node.js 20
- TypeScript
- NestJS
- PostgreSQL
- RabbitMQ
- Docker Compose
- k6

## Quick start

1. Copy `.env.example` to `.env`
2. Install dependencies: `npm install`
3. Build all packages: `npm run build`
4. Start the stack: `docker compose up --build -d`
5. Call the API: `curl http://localhost:3000/health`

## Example request

```bash
curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "c-1001",
    "productId": "sku-001",
    "quantity": 1,
    "totalAmount": 249.99
  }'
```

## Benchmark scripts

- `tests/k6/baseline-synchronous.js`
- `tests/k6/rabbitmq-async.js`

## Documentation

- `docs/architecture.md`
- `docs/benchmark.md`
- `docs/broker-comparison.md`
- `docs/experiments.md`
- `docs/decision.md`

## Important note

This project does not claim a broker is universally best. The decision should be based on measured data collected from the same workloads against each broker.
