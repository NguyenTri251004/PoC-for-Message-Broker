import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { RabbitMQBroker } from '@poc/messaging';
import { OrderEvent } from '@poc/contracts';
import { DatabaseService } from './database.service';

@Injectable()
export class AppService implements OnModuleInit {
  private readonly logger = new Logger(AppService.name);
  private readonly attemptHistory: Array<{ eventId: string; orderId: string; retryCount: number; at: string; decision: string }> = [];
  private broker: RabbitMQBroker | null = null;
  private failureMode: 'off' | 'fail-first-n' | 'always-fail' | 'crash-after-success-once' = 'off';
  private remainingFailures = 0;
  private crashAfterSuccessOnce = false;
  // Test-only override that forces the random failure rate to zero so failure
  // scenarios stay deterministic regardless of PAYMENT_FAILURE_RATE.
  private disableRandomFailures = false;
  private processingDelayMs = Number(process.env.PAYMENT_PROCESSING_DELAY_MS ?? process.env.PAYMENT_DELAY_MS ?? 2000);

  constructor(private readonly databaseService: DatabaseService) {}

  async onModuleInit() {
    try {
      const broker = new RabbitMQBroker(process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672');
      await broker.connect();
      this.broker = broker;
      await broker.subscribe('payment.order.queue', 'order.events', 'order.created', async (msg) => {
        await this.consumeOrderCreated(msg as unknown as OrderEvent);
      });
      this.logger.log('Payment consumer subscribed to order.created');
    } catch (error) {
      this.logger.warn(`Payment consumer unavailable: ${(error as Error).message}`);
    }
  }

  async getHealth() {
    return {
      status: 'ok',
      service: 'payment-service',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      consumer: this.broker ? 'connected' : 'disconnected',
    };
  }

    async authorizePayment(payload: Record<string, unknown>) {
    const rate = this.disableRandomFailures ? 0 : Number(process.env.PAYMENT_FAILURE_RATE ?? 0.0);
    if (Math.random() < rate) {
      throw new Error('Payment service unavailable');
    }

    const traceId = String(payload.traceId ?? 'na');
    const orderId = String(payload.orderId ?? 'pending');
    const eventId = String(payload.eventId ?? crypto.randomUUID());

    // Apply the same deterministic processing delay used by the async consumer so
    // the synchronous baseline and the RabbitMQ path do equivalent work.
    await this.sleep(Number(process.env.PAYMENT_PROCESSING_DELAY_MS ?? process.env.PAYMENT_DELAY_MS ?? this.processingDelayMs));

    // Persist the same durable payment event the async consumer writes, so the
    // benchmark can verify downstream completion identically in both modes.
    const client = this.databaseService.getClient();
    await client.query(
      `INSERT INTO payment_events (event_id, order_id, trace_id, retry_count, status, processed_at, last_attempt_at, last_failure_reason)
       VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NULL)
       ON CONFLICT (event_id) DO UPDATE SET
         status = EXCLUDED.status,
         last_attempt_at = NOW(),
         processed_at = NOW()`,
      [eventId, orderId, traceId, 0, 'payment_processed'],
    );

    return {
      success: true,
      orderId,
      eventId,
      status: 'authorized',
      traceId,
    };
  }

  async consumeOrderCreated(event: OrderEvent) {
    const retryCount = Number(event.retryCount ?? 0);
    const decision = this.shouldFail(event);

    this.attemptHistory.push({
      eventId: String(event.eventId),
      orderId: String(event.orderId),
      retryCount,
      at: new Date().toISOString(),
      decision,
    });

    this.logger.log(`Payment processing orderId=${event.orderId} traceId=${event.traceId} retryCount=${retryCount} eventId=${event.eventId} decision=${decision}`);

    if (decision !== 'process') {
      throw new Error(`Payment ${decision} for ${event.orderId}`);
    }

    await this.sleep(this.processingDelayMs);

    const client = this.databaseService.getClient();
    const existing = await client.query('SELECT event_id, status, retry_count, processed_at, last_attempt_at, last_failure_reason FROM payment_events WHERE event_id = $1', [event.eventId]);

    if ((existing.rowCount ?? 0) > 0 && String(existing.rows[0].status) === 'payment_processed') {
      this.logger.log(`Payment duplicate ignored orderId=${event.orderId} traceId=${event.traceId} eventId=${event.eventId}`);
      return;
    }

    await client.query(
      `INSERT INTO payment_events (event_id, order_id, trace_id, retry_count, status, processed_at, last_attempt_at, last_failure_reason)
       VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NULL)
       ON CONFLICT (event_id) DO UPDATE SET
         retry_count = EXCLUDED.retry_count,
         status = EXCLUDED.status,
         last_attempt_at = NOW(),
         last_failure_reason = NULL,
         processed_at = NOW()`,
      [event.eventId, event.orderId, event.traceId, retryCount, 'payment_processed'],
    );

    if (this.crashAfterSuccessOnce) {
      this.crashAfterSuccessOnce = false;
      this.logger.error(`Payment crash-after-success triggered for eventId=${event.eventId}`);
      process.nextTick(() => process.exit(1));
      return;
    }

    this.logger.log(`Payment processed orderId=${event.orderId} traceId=${event.traceId} eventId=${event.eventId}`);
  }

  getProcessedEvents() {
    return this.getProcessedRows();
  }

  getDebugState() {
    return {
      failureMode: this.failureMode,
      remainingFailures: this.remainingFailures,
      crashAfterSuccessOnce: this.crashAfterSuccessOnce,
      disableRandomFailures: this.disableRandomFailures,
      processingDelayMs: this.processingDelayMs,
      attempts: this.attemptHistory.slice(-50),
    };
  }

  configureDebugState(payload: Record<string, unknown>) {
    const mode = String(payload.mode ?? 'off');
    if (mode === 'fail-first-n') {
      this.failureMode = 'fail-first-n';
      this.remainingFailures = Number(payload.remainingFailures ?? 1);
      this.crashAfterSuccessOnce = false;
    } else if (mode === 'always-fail') {
      this.failureMode = 'always-fail';
      this.remainingFailures = 0;
      this.crashAfterSuccessOnce = false;
    } else if (mode === 'crash-after-success-once') {
      this.failureMode = 'crash-after-success-once';
      this.remainingFailures = 0;
      this.crashAfterSuccessOnce = true;
    } else {
      this.failureMode = 'off';
      this.remainingFailures = 0;
      this.crashAfterSuccessOnce = false;
    }

    if (payload.processingDelayMs !== undefined) {
      this.processingDelayMs = Number(payload.processingDelayMs);
    }

    if (payload.disableRandomFailures !== undefined) {
      this.disableRandomFailures = Boolean(payload.disableRandomFailures);
    }

    return this.getDebugState();
  }

  async resetDebugState() {
    this.attemptHistory.length = 0;
    const client = this.databaseService.getClient();
    await client.query('DELETE FROM payment_events');
    return this.getDebugState();
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private shouldFail(event: OrderEvent) {
    if (this.failureMode === 'always-fail') {
      return 'always-fail';
    }

    if (this.failureMode === 'fail-first-n') {
      if (this.remainingFailures > 0) {
        this.remainingFailures -= 1;
        return 'fail-first-n';
      }
    }

    if (this.failureMode === 'crash-after-success-once') {
      return 'process';
    }

    const configuredRate = this.disableRandomFailures ? 0 : Number(process.env.PAYMENT_FAILURE_RATE ?? 0);
    const maxRetries = Number(process.env.PAYMENT_MAX_RETRIES ?? 3);
    if (configuredRate > 0 && Number(event.retryCount ?? 0) < maxRetries && Math.random() < configuredRate) {
      return 'random-fail';
    }

    return 'process';
  }

  private async getProcessedRows() {
    const client = this.databaseService.getClient();
    const rows = await client.query('SELECT event_id AS "eventId", order_id AS "orderId", trace_id AS "traceId", retry_count AS "retryCount", status, processed_at AS "processedAt", last_attempt_at AS "lastAttemptAt", last_failure_reason AS "lastFailureReason" FROM payment_events ORDER BY processed_at DESC LIMIT 50');
    return { count: rows.rowCount, events: rows.rows };
  }
}
