# RabbitMQ Failure-Mode Test Summary

Generated: 2026-09-28T09:44:47.553Z

Totals: PASS=7 FAIL=0 BLOCKED=0

| Test | Scenario | Expected behavior | Actual behavior | Status | Evidence |
| ---- | -------- | ----------------- | --------------- | ------ | -------- |
| A | Payment service unavailable | Order service accepts and persists while payment is down; the event stays in RabbitMQ and is processed exactly once after restart. | Order accepted and persisted while payment was stopped; the event remained queued and was processed exactly once after restart. | PASS | tests/results/failure/payment-unavailable.json |
| B | Intermittent payment failure with retry | First N attempts fail, retries increase, the message is preserved and eventually processed exactly once without blocking the order. | Payment failed 2 times, was retried through the broker, and succeeded once with a single durable record (retry_count=2). | PASS | tests/results/failure/payment-retry.json |
| C | Poison payment message and DLQ | A permanently failing message is retried up to the configured limit then moved to the DLQ; no infinite retry and consumers stay healthy. | Message retried up to the 3-retry limit (attempts observed: 4), main queue drained, message dead-lettered to payment.order.queue.dlq. | PASS | tests/results/failure/payment-dlq.json |
| D | Consumer crash and restart | Crashing the consumer after a durable write causes RabbitMQ to redeliver the unacked message; idempotency prevents a duplicate business effect. | Payment consumer crashed after the durable write, was restarted, redelivered the message, and the business record remained single-copy. | PASS | tests/results/failure/consumer-crash-recovery.json |
| E | Duplicate message and idempotency | The same logical event delivered twice decrements stock exactly once; the duplicate is recognised and ignored for both inventory and payment. | Stock decreased exactly once (1000 -> 998) and remained unchanged after the duplicate (998); payment had a single row. | PASS | tests/results/failure/duplicate-idempotency.json |
| F | Inventory concurrency and overselling | Concurrent reservations never drive stock negative and never exceed available stock; the DB remains consistent with no lost updates. | Initial stock 10; 5 of 8 reservations of 2 succeeded (10 units), 3 rejected; final available=0, reserved=10; no overselling or lost updates. | PASS | tests/results/failure/inventory-concurrency.json |
| G | Slow notification and analytics | Order acceptance and persistence remain fast while slow Notification/Analytics consumers process asynchronously. | Order API responded in 114.9ms (below the 3000ms consumer delay); order persisted immediately; payment/inventory proceeded and slow consumers completed later. | PASS | tests/results/failure/slow-consumers.json |

> Status is PASS only when verified by actual runtime evidence captured in the linked JSON file.
> Failure injection is development-only and disabled by default.
