export type BrokerType = 'rabbitmq' | 'activemq' | 'kafka';
export interface HealthResponse {
    status: 'ok' | 'degraded';
    service: string;
    timestamp: string;
    version: string;
}
export interface CreateOrderRequest {
    customerId: string;
    productId: string;
    quantity: number;
    totalAmount: number;
}
export interface CreateOrderResponse {
    success: boolean;
    orderId: string;
    status: 'accepted' | 'created' | 'failed';
    message: string;
    mode: 'sync' | 'async';
}
export interface OrderEvent {
    eventId: string;
    orderId: string;
    customerId: string;
    productId: string;
    quantity: number;
    totalAmount: number;
    timestamp: string;
    traceId: string;
    type: string;
    retryCount?: number;
    reason?: string;
    remainingStock?: number;
    source?: string;
    payload?: Record<string, unknown>;
}
