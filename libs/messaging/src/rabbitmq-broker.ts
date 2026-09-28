import * as amqp from 'amqplib';
import { MessageBroker } from './message-broker.interface';

export class RabbitMQBroker implements MessageBroker {
  private connection: any = null;
  private channel: any = null;
  private readonly url: string;
  // Exchanges are idempotent; assert each one once per connection instead of on
  // every publish (asserting on the hot path adds a broker round-trip per
  // message and serialises throughput on the shared channel).
  private readonly assertedExchanges = new Set<string>();

  constructor(url: string) {
    this.url = url;
  }

  async connect(): Promise<void> {
    let attempt = 0;
    const maxAttempts = Number(process.env.RABBITMQ_CONNECT_ATTEMPTS ?? 30);
    const retryDelayMs = Number(process.env.RABBITMQ_CONNECT_RETRY_DELAY_MS ?? 2000);

    while (true) {
      try {
        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();
        await this.channel.prefetch(1);
        return;
      } catch (error) {
        attempt += 1;

        if (attempt >= maxAttempts) {
          throw error;
        }

        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
    }
  }

  async publish(exchange: string, routingKey: string, message: Record<string, unknown>, options: Record<string, unknown> = {}): Promise<void> {
        if (!this.channel) {
      throw new Error('RabbitMQ channel is not initialized');
    }

    if (!this.assertedExchanges.has(exchange)) {
      await this.channel.assertExchange(exchange, 'topic', { durable: true });
      this.assertedExchanges.add(exchange);
    }

    this.channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(message)), {
      persistent: true,
      ...options,
    });
  }

  async subscribe(queueName: string, exchange: string, routingKey: string, handler: (msg: Record<string, unknown>, raw?: unknown) => Promise<void>): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel is not initialized');
    }

    const maxRetries = Number(process.env.MESSAGE_RETRY_LIMIT ?? 3);

    await this.channel.assertExchange(exchange, 'topic', { durable: true });
    await this.channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': 'dlx.order.events',
        'x-dead-letter-routing-key': `${queueName}.dlq`,
      },
    });
    await this.channel.bindQueue(queueName, exchange, routingKey);

    await this.channel.assertExchange('dlx.order.events', 'direct', { durable: true });
    await this.channel.assertQueue(`${queueName}.dlq`, { durable: true });
    await this.channel.bindQueue(`${queueName}.dlq`, 'dlx.order.events', `${queueName}.dlq`);

    await this.channel.consume(queueName, async (msg: any) => {
      if (!msg) {
        return;
      }

      try {
        const parsed = JSON.parse(msg.content.toString('utf8')) as Record<string, unknown>;
        await handler(parsed, msg);
        await this.ack(msg);
      } catch (error) {
        const parsed = JSON.parse(msg.content.toString('utf8')) as Record<string, unknown>;
        const retryCount = Number(parsed.retryCount ?? 0);
        const nextRetryCount = retryCount + 1;

        if (nextRetryCount <= maxRetries) {
          parsed.retryCount = nextRetryCount;
          parsed.reason = (error as Error).message;
          parsed.lastFailedAt = new Date().toISOString();
          this.channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(parsed)), { persistent: true });
          console.warn(`[${queueName}] retry ${nextRetryCount}/${maxRetries} for order ${String(parsed.orderId ?? 'unknown')} traceId=${String(parsed.traceId ?? 'unknown')}`);
          await this.ack(msg);
          return;
        }

        console.error(`[${queueName}] processing failed after ${maxRetries} retries`, error);
        await this.nack(msg, false);
      }
    }, { noAck: false });
  }

  async ack(raw: unknown): Promise<void> {
    if (this.channel && raw) {
      this.channel.ack(raw as any);
    }
  }

  async nack(raw: unknown, requeue = false): Promise<void> {
    if (this.channel && raw) {
      this.channel.nack(raw as any, false, requeue);
    }
  }

  async close(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();
  }
}

