import { Injectable, OnModuleInit } from '@nestjs/common';
import { RabbitMQBroker } from '@poc/messaging';

@Injectable()
export class MessageBrokerService implements OnModuleInit {
  broker: RabbitMQBroker | null = null;

  async onModuleInit() {
    if ((process.env.MESSAGE_BROKER ?? 'rabbitmq').toLowerCase() !== 'rabbitmq') {
      return;
    }

    try {
      this.broker = new RabbitMQBroker(process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672');
      await this.broker.connect();
    } catch (error) {
      console.warn('[order-service] RabbitMQ broker not ready yet, will retry on order publish:', (error as Error).message);
    }
  }

  async publishOrderCreated(event: Record<string, unknown>) {
    if (!this.broker) {
      this.broker = new RabbitMQBroker(process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672');
      await this.broker.connect();
    }

    await this.broker.publish('order.events', 'order.created', event);
  }
}
