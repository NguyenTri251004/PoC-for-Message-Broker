import { Body, Controller, Get, Headers, Post } from '@nestjs/common';
import { CreateOrderRequest, CreateOrderResponse, HealthResponse } from '@poc/contracts';
import { OrderService } from './order.service';

@Controller()
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Get('/health')
  async health(): Promise<HealthResponse> {
    return this.orderService.getHealth();
  }

  @Post('/orders')
  async createOrder(
    @Body() dto: CreateOrderRequest,
    @Headers('x-benchmark-mode') benchmarkMode?: string,
  ): Promise<CreateOrderResponse> {
    return this.orderService.createOrder(dto, benchmarkMode);
  }
}

