import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { RabbitMQBroker } from '@poc/messaging';
import { OrderEvent } from '@poc/contracts';

@Injectable()
export class AppService implements OnModuleInit {
  private readonly logger = new Logger(AppService.name);
  private broker: RabbitMQBroker | null = null;
  private sent: Array<{ orderId: string; at: string; traceId?: string }> = [];
  private processingDelayMs = Number(process.env.NOTIFICATION_PROCESSING_DELAY_MS ?? process.env.NOTIFICATION_DELAY_MS ?? 1000);

  async onModuleInit() {
    try {
      const broker = new RabbitMQBroker(process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672');
      await broker.connect();
      this.broker = broker;
      await broker.subscribe('notification.order.queue', 'order.events', 'order.created', async (msg) => {
        await this.consumeOrderCreated(msg as unknown as OrderEvent);
      });
      this.logger.log('Notification consumer subscribed to order.created');
    } catch (error) {
      this.logger.warn(`Notification consumer unavailable: ${(error as Error).message}`);
    }
  }

  async getHealth() {
    return {
      status: 'ok',
      service: 'notification-service',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      consumer: this.broker ? 'connected' : 'disconnected',
    };
  }

  async send(payload: Record<string, unknown>) {
    await this.sleep(this.processingDelayMs);
    this.sent.push({
      orderId: String(payload.orderId ?? 'unknown'),
      at: new Date().toISOString(),
      traceId: String(payload.traceId ?? 'unknown'),
    });

    return {
      success: true,
      orderId: String(payload.orderId ?? 'unknown'),
      status: 'notification_sent',
    };
  }

  async consumeOrderCreated(event: OrderEvent) {
    this.logger.log(`Notification received orderId=${event.orderId} traceId=${event.traceId}`);
    await this.sleep(this.processingDelayMs);
    this.sent.push({
      orderId: event.orderId,
      at: new Date().toISOString(),
      traceId: event.traceId,
    });
    this.logger.log(`Notification sent for orderId=${event.orderId} traceId=${event.traceId}`);
  }

  getSent() {
    return { count: this.sent.length, items: this.sent.slice(-20), processingDelayMs: this.processingDelayMs };
  }

  configureDebugState(payload: Record<string, unknown>) {
    if (payload.processingDelayMs !== undefined) {
      this.processingDelayMs = Number(payload.processingDelayMs);
    }

    return this.getSent();
  }

  resetDebugState() {
    this.sent = [];
    return this.getSent();
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
