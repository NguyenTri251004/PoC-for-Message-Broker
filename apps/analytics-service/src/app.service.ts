import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { RabbitMQBroker } from '@poc/messaging';
import { OrderEvent } from '@poc/contracts';

@Injectable()
export class AppService implements OnModuleInit {
  private readonly logger = new Logger(AppService.name);
  private broker: RabbitMQBroker | null = null;
  private tracked: Array<{ orderId: string; at: string; traceId?: string }> = [];
  private processingDelayMs = Number(process.env.ANALYTICS_PROCESSING_DELAY_MS ?? process.env.ANALYTICS_DELAY_MS ?? 500);

  async onModuleInit() {
    try {
      const broker = new RabbitMQBroker(process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672');
      await broker.connect();
      this.broker = broker;
      await broker.subscribe('analytics.order.queue', 'order.events', 'order.created', async (msg) => {
        await this.consumeOrderCreated(msg as unknown as OrderEvent);
      });
      this.logger.log('Analytics consumer subscribed to order.created');
    } catch (error) {
      this.logger.warn(`Analytics consumer unavailable: ${(error as Error).message}`);
    }
  }

  async getHealth() {
    return {
      status: 'ok',
      service: 'analytics-service',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      consumer: this.broker ? 'connected' : 'disconnected',
    };
  }

  async track(payload: Record<string, unknown>) {
    await this.sleep(this.processingDelayMs);
    this.tracked.push({
      orderId: String(payload.orderId ?? 'unknown'),
      at: new Date().toISOString(),
      traceId: String(payload.traceId ?? 'unknown'),
    });

    return {
      success: true,
      orderId: String(payload.orderId ?? 'unknown'),
      status: 'tracked',
    };
  }

  async consumeOrderCreated(event: OrderEvent) {
    this.logger.log(`Analytics received orderId=${event.orderId} traceId=${event.traceId}`);
    await this.sleep(this.processingDelayMs);
    this.tracked.push({
      orderId: event.orderId,
      at: new Date().toISOString(),
      traceId: event.traceId,
    });
    this.logger.log(`Analytics tracked orderId=${event.orderId} traceId=${event.traceId}`);
  }

  getTracked() {
    return { count: this.tracked.length, items: this.tracked.slice(-20), processingDelayMs: this.processingDelayMs };
  }

  configureDebugState(payload: Record<string, unknown>) {
    if (payload.processingDelayMs !== undefined) {
      this.processingDelayMs = Number(payload.processingDelayMs);
    }

    return this.getTracked();
  }

  resetDebugState() {
    this.tracked = [];
    return this.getTracked();
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
