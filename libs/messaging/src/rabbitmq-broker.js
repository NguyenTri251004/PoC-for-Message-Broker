"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.RabbitMQBroker = void 0;
const amqp = __importStar(require("amqplib"));
class RabbitMQBroker {
    connection = null;
    channel = null;
    url;
    constructor(url) {
        this.url = url;
    }
    async connect() {
        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();
        await this.channel.prefetch(1);
    }
    async publish(exchange, routingKey, message, options = {}) {
        if (!this.channel) {
            throw new Error('RabbitMQ channel is not initialized');
        }
        await this.channel.assertExchange(exchange, 'topic', { durable: true });
        this.channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(message)), {
            persistent: true,
            ...options,
        });
    }
    async subscribe(queueName, exchange, routingKey, handler) {
        if (!this.channel) {
            throw new Error('RabbitMQ channel is not initialized');
        }
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
        await this.channel.consume(queueName, async (msg) => {
            if (!msg) {
                return;
            }
            try {
                const parsed = JSON.parse(msg.content.toString('utf8'));
                await handler(parsed, msg);
                await this.ack(msg);
            }
            catch (error) {
                console.error(`[${queueName}] processing failed`, error);
                await this.nack(msg, false);
            }
        }, { noAck: false });
    }
    async ack(raw) {
        if (this.channel && raw) {
            this.channel.ack(raw);
        }
    }
    async nack(raw, requeue = false) {
        if (this.channel && raw) {
            this.channel.nack(raw, false, requeue);
        }
    }
    async close() {
        await this.channel?.close();
        await this.connection?.close();
    }
}
exports.RabbitMQBroker = RabbitMQBroker;
//# sourceMappingURL=rabbitmq-broker.js.map