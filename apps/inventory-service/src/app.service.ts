import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { RabbitMQBroker } from '@poc/messaging';
import { OrderEvent } from '@poc/contracts';
import { DatabaseService } from './database.service';

@Injectable()
export class AppService implements OnModuleInit {
  private readonly logger = new Logger(AppService.name);
  private broker: RabbitMQBroker | null = null;
  private failureMode: 'off' | 'always-fail' | 'fail-first-n' = 'off';
  private remainingFailures = 0;
  private readonly attemptHistory: Array<{ eventId: string; orderId: string; retryCount: number; at: string; decision: string }> = [];

  constructor(private readonly databaseService: DatabaseService) {}

  async onModuleInit() {
    try {
      const broker = new RabbitMQBroker(process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672');
      await broker.connect();
      this.broker = broker;
      await broker.subscribe('inventory.order.queue', 'order.events', 'order.created', async (msg) => {
        await this.consumeOrderCreated(msg as unknown as OrderEvent);
      });
      this.logger.log('Inventory consumer subscribed to order.created');
    } catch (error) {
      this.logger.warn(`Inventory consumer unavailable: ${(error as Error).message}`);
    }
  }

  async getHealth() {
    const stock = await this.getStock();
    return {
      status: 'ok',
      service: 'inventory-service',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      consumer: this.broker ? 'connected' : 'disconnected',
      stock: stock.available,
    };
  }

    async reserve(payload: Record<string, unknown>) {
    const productId = String(payload.productId ?? 'sku-001');
    const quantity = Number(payload.quantity ?? 0);
    const eventId = String(payload.eventId ?? crypto.randomUUID());
    const orderId = String(payload.orderId ?? '00000000-0000-0000-0000-000000000000');
    const traceId = String(payload.traceId ?? 'na');

    // Apply the same deterministic processing delay used by the async consumer so
    // the synchronous baseline and the RabbitMQ path do equivalent work.
    await this.sleep(Number(process.env.INVENTORY_PROCESSING_DELAY_MS ?? process.env.INVENTORY_DELAY_MS ?? 100));

        const client = await this.databaseService.getClient();

    try {
      await client.query('BEGIN');

      const duplicate = await client.query('SELECT event_id FROM inventory_events WHERE event_id = $1', [eventId]);
      if ((duplicate.rowCount ?? 0) > 0) {
        await client.query('COMMIT');
        return {
          success: true,
          productId,
          duplicate: true,
        };
      }

      const result = await client.query(
        `UPDATE inventory_items
         SET available = available - $1,
             reserved = reserved + $1,
             updated_at = NOW()
         WHERE product_id = $2 AND available >= $1
         RETURNING available, reserved`,
        [quantity, productId],
      );

      if (result.rowCount === 0) {
        await client.query('ROLLBACK');
        throw new Error('Insufficient stock');
      }

      await client.query(
        `INSERT INTO inventory_events (event_id, order_id, product_id, quantity, trace_id, status)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [eventId, orderId, productId, quantity, traceId, 'reserved'],
      );

      await client.query('COMMIT');

      return {
        success: true,
        productId,
        remainingStock: Number(result.rows[0].available),
        reserved: Number(result.rows[0].reserved),
      };
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ignore rollback failure, surface original error
      }
      throw error;
    } finally {
      this.databaseService.releaseClient(client);
    }
  }

  async consumeOrderCreated(event: OrderEvent) {
    const retryCount = Number(event.retryCount ?? 0);
    const decision = this.shouldFail();

    this.attemptHistory.push({
      eventId: String(event.eventId),
      orderId: String(event.orderId),
      retryCount,
      at: new Date().toISOString(),
      decision,
    });

    this.logger.log(`Inventory received orderId=${event.orderId} traceId=${event.traceId} productId=${event.productId} quantity=${event.quantity} retryCount=${retryCount} decision=${decision}`);

    if (decision !== 'process') {
      throw new Error(`Inventory ${decision} for ${event.orderId}`);
    }

        const client = await this.databaseService.getClient();

    try {
      await client.query('BEGIN');

      const duplicate = await client.query('SELECT event_id FROM inventory_events WHERE event_id = $1', [event.eventId]);
      if ((duplicate.rowCount ?? 0) > 0) {
        await client.query('COMMIT');
        this.logger.log(`Inventory duplicate ignored orderId=${event.orderId} traceId=${event.traceId} eventId=${event.eventId}`);
        return;
      }

      const update = await client.query(
        `UPDATE inventory_items
         SET available = available - $1,
             reserved = reserved + $1,
             updated_at = NOW()
         WHERE product_id = $2 AND available >= $1
         RETURNING available, reserved`,
        [Number(event.quantity ?? 0), event.productId],
      );

      if (update.rowCount === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Insufficient stock for ${event.productId}`);
      }

      await client.query(
        `INSERT INTO inventory_events (event_id, order_id, product_id, quantity, trace_id, status)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [event.eventId, event.orderId, event.productId, Number(event.quantity ?? 0), event.traceId, 'reserved'],
      );

      await client.query('COMMIT');

      this.logger.log(`Reserved ${event.quantity} for ${event.productId}; remaining ${Number(update.rows[0].available)}`);
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ignore rollback failure, surface original error
      }
      throw error;
    } finally {
      this.databaseService.releaseClient(client);
    }
  }

    async getStock() {
    const client = await this.databaseService.getClient();
    try {
      const result = await client.query('SELECT product_id AS "productId", available, reserved FROM inventory_items WHERE product_id = $1', ['sku-001']);

      if (result.rowCount === 0) {
        return { productId: 'sku-001', available: 0, reserved: 0 };
      }

      return result.rows[0] as { productId: string; available: number; reserved: number };
    } finally {
      this.databaseService.releaseClient(client);
    }
  }

  getDebugState() {
    return {
      failureMode: this.failureMode,
      remainingFailures: this.remainingFailures,
      attempts: this.attemptHistory.slice(-50),
    };
  }

  configureDebugState(payload: Record<string, unknown>) {
    const mode = String(payload.mode ?? 'off');
    if (mode === 'fail-first-n') {
      this.failureMode = 'fail-first-n';
      this.remainingFailures = Number(payload.remainingFailures ?? 1);
    } else if (mode === 'always-fail') {
      this.failureMode = 'always-fail';
      this.remainingFailures = 0;
    } else {
      this.failureMode = 'off';
      this.remainingFailures = 0;
    }

    return this.getDebugState();
  }

    async resetDebugState() {
    this.attemptHistory.length = 0;
    const client = await this.databaseService.getClient();
    try {
      await client.query('BEGIN');
      await client.query('DELETE FROM inventory_events');
      await client.query('UPDATE inventory_items SET available = 1000, reserved = 0, updated_at = NOW() WHERE product_id = $1', ['sku-001']);
      await client.query('COMMIT');
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ignore rollback failure, surface original error
      }
      throw error;
    } finally {
      this.databaseService.releaseClient(client);
    }

    return this.getDebugState();
  }

    private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private shouldFail() {
    if (this.failureMode === 'always-fail') {
      return 'always-fail';
    }

    if (this.failureMode === 'fail-first-n' && this.remainingFailures > 0) {
      this.remainingFailures -= 1;
      return 'fail-first-n';
    }

    return 'process';
  }
}
