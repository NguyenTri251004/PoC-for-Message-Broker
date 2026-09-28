import { Injectable, Logger } from '@nestjs/common';
import { CreateOrderRequest, CreateOrderResponse, HealthResponse } from '@poc/contracts';
import { buildTraceId } from '@poc/common';
import { DatabaseService } from './database.service';
import { MessageBrokerService } from './message-broker.service';

/**
 * Processing mode for an order.
 * - `async` : persist + publish to the broker, respond immediately (RabbitMQ path).
 * - `sync`  : persist + orchestrate the downstream services over HTTP and only
 *             respond once the whole chain has completed (synchronous baseline).
 */
export type OrderMode = 'async' | 'sync';

function resolveMode(requested?: string): OrderMode {
  if (requested === 'sync' || requested === 'async') {
    return requested;
  }
  // Fall back to the static configuration toggle for backwards compatibility.
  const configured = (process.env.USE_ASYNC_BROKER ?? 'true').toLowerCase();
  return configured === 'false' ? 'sync' : 'async';
}

@Injectable()
export class OrderService {
  private readonly logger = new Logger(OrderService.name);
  constructor(
    private readonly databaseService: DatabaseService,
    private readonly messageBrokerService: MessageBrokerService,
  ) {}

  async getHealth(): Promise<HealthResponse> {
    return {
      status: 'ok',
      service: 'order-service',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
    };
  }

  async createOrder(payload: CreateOrderRequest, requestedMode?: string): Promise<CreateOrderResponse> {
    const mode = resolveMode(requestedMode);
    const orderId = crypto.randomUUID();
    const traceId = buildTraceId();
    const eventId = crypto.randomUUID();
    const client = await this.databaseService.getClient();

    try {
      await client.query(
        'INSERT INTO orders (id, customer_id, product_id, quantity, total_amount, status) VALUES ($1, $2, $3, $4, $5, $6)',
        [orderId, payload.customerId, payload.productId, payload.quantity, payload.totalAmount, 'created'],
      );

      if (mode === 'async') {
        await this.messageBrokerService.publishOrderCreated({
          eventId,
          type: 'OrderCreated',
          orderId,
          customerId: payload.customerId,
          productId: payload.productId,
          quantity: payload.quantity,
          totalAmount: payload.totalAmount,
          timestamp: new Date().toISOString(),
          traceId,
        });

        this.logger.log(`Order created (async): orderId=${orderId} traceId=${traceId} eventId=${eventId}`);

        return {
          success: true,
          orderId,
          eventId,
          traceId,
          status: 'accepted',
          message: 'Order accepted for async processing',
          mode: 'async',
        };
      }

      await this.executeSyncFlow(payload, orderId, eventId, traceId);

      await client.query('UPDATE orders SET status = $1 WHERE id = $2', ['completed', orderId]);
    } finally {
      this.databaseService.releaseClient(client);
    }

    this.logger.log(`Order created (sync): orderId=${orderId} traceId=${traceId}`);

    return {
      success: true,
      orderId,
      eventId,
      traceId,
      status: 'created',
      message: 'Order created synchronously',
      mode: 'sync',
    };
  }

  /**
   * Synchronous orchestration of the downstream chain.
   *
   * The downstream HTTP endpoints apply their OWN processing delay (the same
   * `*_PROCESSING_DELAY_MS` values used by the RabbitMQ consumers). We therefore
   * do NOT add extra sleeps here, otherwise the delay would be double-counted
   * and the comparison against the async path would be unfair.
   */
  private async executeSyncFlow(
    payload: CreateOrderRequest,
    orderId: string,
    eventId: string,
    traceId: string,
  ): Promise<{ paymentMs: number; inventoryMs: number; notificationMs: number; analyticsMs: number }> {
    const paymentUrl = process.env.PAYMENT_SERVICE_URL ?? 'http://localhost:3002';
    const inventoryUrl = process.env.INVENTORY_SERVICE_URL ?? 'http://localhost:3003';
    const notificationUrl = process.env.NOTIFICATION_SERVICE_URL ?? 'http://localhost:3004';
    const analyticsUrl = process.env.ANALYTICS_SERVICE_URL ?? 'http://localhost:3005';

    const paymentMs = await this.postJson(`${paymentUrl}/payments/authorize`, {
      eventId,
      orderId,
      customerId: payload.customerId,
      productId: payload.productId,
      quantity: payload.quantity,
      totalAmount: payload.totalAmount,
      traceId,
    }, 'payment');

    const inventoryMs = await this.postJson(`${inventoryUrl}/inventory/reserve`, {
      eventId,
      orderId,
      productId: payload.productId,
      quantity: payload.quantity,
      traceId,
    }, 'inventory');

    const notificationMs = await this.postJson(`${notificationUrl}/notifications/send`, {
      eventId,
      orderId,
      customerId: payload.customerId,
      productId: payload.productId,
      traceId,
    }, 'notification');

    const analyticsMs = await this.postJson(`${analyticsUrl}/analytics/track`, {
      eventId,
      orderId,
      customerId: payload.customerId,
      productId: payload.productId,
      quantity: payload.quantity,
      totalAmount: payload.totalAmount,
      traceId,
    }, 'analytics');

    return { paymentMs, inventoryMs, notificationMs, analyticsMs };
  }

  private async postJson(url: string, body: Record<string, unknown>, stage: string): Promise<number> {
    const startedAt = Date.now();
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`[order-service] ${stage} failed (${response.status}): ${text}`);
    }

    await response.json();
    return Date.now() - startedAt;
  }
}
