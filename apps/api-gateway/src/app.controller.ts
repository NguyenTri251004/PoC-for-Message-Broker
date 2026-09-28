import { Body, Controller, Get, Headers, Post } from '@nestjs/common';
import { CreateOrderRequest, CreateOrderResponse, HealthResponse } from '@poc/contracts';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/health')
  async health(): Promise<HealthResponse> {
    return this.appService.getHealth();
  }

  @Post('/orders')
  async createOrder(
    @Body() dto: CreateOrderRequest,
    @Headers('x-benchmark-mode') benchmarkMode?: string,
  ): Promise<CreateOrderResponse> {
    return this.appService.createOrder(dto, benchmarkMode);
  }
}

