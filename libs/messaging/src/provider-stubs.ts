import { MessageBroker } from './message-broker.interface';

export class ActiveMQBroker implements MessageBroker {
  async connect(): Promise<void> { throw new Error('ActiveMQ adapter not implemented'); }
  async publish(): Promise<void> { throw new Error('ActiveMQ adapter not implemented'); }
  async subscribe(): Promise<void> { throw new Error('ActiveMQ adapter not implemented'); }
  async ack(): Promise<void> { throw new Error('ActiveMQ adapter not implemented'); }
  async nack(): Promise<void> { throw new Error('ActiveMQ adapter not implemented'); }
  async close(): Promise<void> { throw new Error('ActiveMQ adapter not implemented'); }
}

export class KafkaBroker implements MessageBroker {
  async connect(): Promise<void> { throw new Error('Kafka adapter not implemented'); }
  async publish(): Promise<void> { throw new Error('Kafka adapter not implemented'); }
  async subscribe(): Promise<void> { throw new Error('Kafka adapter not implemented'); }
  async ack(): Promise<void> { throw new Error('Kafka adapter not implemented'); }
  async nack(): Promise<void> { throw new Error('Kafka adapter not implemented'); }
  async close(): Promise<void> { throw new Error('Kafka adapter not implemented'); }
}
