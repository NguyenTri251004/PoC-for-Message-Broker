import { Injectable, Logger } from '@nestjs/common';
import { CreateOrderRequest, CreateOrderResponse, HealthResponse } from '@poc/contracts';

@Injectable()
export class AppService {
  private readonly logger = new Logger(AppService.name);

  async getHealth(): Promise<HealthResponse> {
    return {
      status: 'ok',
      service: 'api-gateway',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
    };
  }

  async createOrder(payload: CreateOrderRequest, benchmarkMode?: string): Promise<CreateOrderResponse> {
    const orderServiceUrl = process.env.ORDER_SERVICE_URL ?? 'http://localhost:3001';
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (benchmarkMode) {
      headers['x-benchmark-mode'] = benchmarkMode;
    }

    const response = await fetch(`${orderServiceUrl}/orders`, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      this.logger.error(text);
      throw new Error(text);
    }

    return (await response.json()) as CreateOrderResponse;
  }
}

