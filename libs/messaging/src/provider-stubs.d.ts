import { MessageBroker } from './message-broker.interface';
export declare class ActiveMQBroker implements MessageBroker {
    connect(): Promise<void>;
    publish(): Promise<void>;
    subscribe(): Promise<void>;
    ack(): Promise<void>;
    nack(): Promise<void>;
    close(): Promise<void>;
}
export declare class KafkaBroker implements MessageBroker {
    connect(): Promise<void>;
    publish(): Promise<void>;
    subscribe(): Promise<void>;
    ack(): Promise<void>;
    nack(): Promise<void>;
    close(): Promise<void>;
}
