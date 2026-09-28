# Benchmark template

This file serves as a measurement template for the PoC. Replace the `TBD` fields with real numbers from the execution of the k6 scripts on your machine or CI environment.

| Metric | Baseline sync | RabbitMQ async | ActiveMQ | Kafka |
| --- | --- | --- | --- | --- |
| p50 latency | TBD | TBD | TBD | TBD |
| p95 latency | TBD | TBD | TBD | TBD |
| p99 latency | TBD | TBD | TBD | TBD |
| throughput (req/s) | TBD | TBD | TBD | TBD |
| error rate | TBD | TBD | TBD | TBD |
| queue backlog | TBD | TBD | TBD | TBD |
| recovery time after failure | TBD | TBD | TBD | TBD |

## Execution steps

1. Start the services with Docker Compose.
2. Run `k6 run tests/k6/baseline-synchronous.js`.
3. Run `k6 run tests/k6/rabbitmq-async.js`.
4. Capture summaries and compare the metrics above.
