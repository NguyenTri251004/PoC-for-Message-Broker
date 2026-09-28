import { MessageBroker } from './message-broker.interface';
export declare class RabbitMQBroker implements MessageBroker {
    private connection;
    private channel;
    private readonly url;
    constructor(url: string);
    connect(): Promise<void>;
    publish(exchange: string, routingKey: string, message: Record<string, unknown>, options?: Record<string, unknown>): Promise<void>;
    subscribe(queueName: string, exchange: string, routingKey: string, handler: (msg: Record<string, unknown>, raw?: unknown) => Promise<void>): Promise<void>;
    ack(raw: unknown): Promise<void>;
    nack(raw: unknown, requeue?: boolean): Promise<void>;
    close(): Promise<void>;
}
