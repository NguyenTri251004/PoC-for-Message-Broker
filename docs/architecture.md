# Architecture

The system is built as a broker-agnostic microservice cluster. Each domain service owns its own responsibilities, and the broker adapter is isolated behind a shared abstraction.

## Request flow

1. Client submits an order to the API Gateway.
2. The API Gateway forwards the request to the Order Service.
3. The Order Service stores the order and either:
   - calls downstream services synchronously in the baseline mode, or
   - emits an event to RabbitMQ in the async mode.
4. Downstream consumers process domain-specific tasks such as payment authorization, inventory reservation, notification delivery, and analytics tracking.

## Baseline synchronous flow

The synchronous version calls downstream services directly over HTTP. It easily exposes the latency amplification from chained waits.

## RabbitMQ async flow

The async version publishes an `OrderCreated` event to a topic exchange. Consumers listen on dedicated queues and process the message independently.

## Broker abstraction

The code under `libs/messaging` exposes a common interface so RabbitMQ is the active implementation while ActiveMQ and Kafka remain placeholders for future comparison.
