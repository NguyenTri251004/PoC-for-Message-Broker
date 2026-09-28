# Experiment plan

The PoC includes a list of scenarios to compare the behavior of the synchronous and async flows under realistic failure conditions.

## Flow experiments

1. Payment service delay
2. Payment service outage
3. Inventory stock exhaustion
4. Consumer crash during processing
5. Duplicate message replay
6. RabbitMQ downtime and recovery
7. High concurrency with several orders in flight

## Observability checklist

- service health endpoints
- queue depth and consumer status
- logs with trace IDs
- order status transitions
- retry and dead-letter counts

## Acceptance criteria

The system should show stable behavior under transient failures without corrupting order state or allowing negative stock.
