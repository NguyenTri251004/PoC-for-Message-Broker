# Broker comparison

## Implemented

- RabbitMQ adapter
- Broker interface and event abstraction
- Docker Compose wiring

## Planned

- ActiveMQ adapter
- Kafka adapter

## Evaluation criteria

- End-to-end latency
- Throughput under load
- Error handling and recovery
- Message ordering guarantees
- Duplicate handling and idempotency
- Operational complexity
- Observability and tooling quality

## Decision rule

The recommendation should be based on measured performance under the same workload, not assumptions about any one broker.
