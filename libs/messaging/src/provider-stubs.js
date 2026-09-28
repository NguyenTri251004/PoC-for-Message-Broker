"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaBroker = exports.ActiveMQBroker = void 0;
class ActiveMQBroker {
    async connect() { throw new Error('ActiveMQ adapter not implemented'); }
    async publish() { throw new Error('ActiveMQ adapter not implemented'); }
    async subscribe() { throw new Error('ActiveMQ adapter not implemented'); }
    async ack() { throw new Error('ActiveMQ adapter not implemented'); }
    async nack() { throw new Error('ActiveMQ adapter not implemented'); }
    async close() { throw new Error('ActiveMQ adapter not implemented'); }
}
exports.ActiveMQBroker = ActiveMQBroker;
class KafkaBroker {
    async connect() { throw new Error('Kafka adapter not implemented'); }
    async publish() { throw new Error('Kafka adapter not implemented'); }
    async subscribe() { throw new Error('Kafka adapter not implemented'); }
    async ack() { throw new Error('Kafka adapter not implemented'); }
    async nack() { throw new Error('Kafka adapter not implemented'); }
    async close() { throw new Error('Kafka adapter not implemented'); }
}
exports.KafkaBroker = KafkaBroker;
//# sourceMappingURL=provider-stubs.js.map