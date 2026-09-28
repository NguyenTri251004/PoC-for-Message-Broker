import { Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/health')
  async health() {
    return this.appService.getHealth();
  }

  @Post('/payments/authorize')
  async authorizePayment(@Body() payload: Record<string, unknown>) {
    return this.appService.authorizePayment(payload);
  }

  @Get('/debug/processed')
  async processedEvents() {
    return this.appService.getProcessedEvents();
  }

  @Get('/debug/state')
  async state() {
    return this.appService.getDebugState();
  }

  @Post('/debug/config')
  async config(@Body() payload: Record<string, unknown>) {
    return this.appService.configureDebugState(payload);
  }

  @Post('/debug/reset')
  async reset() {
    return this.appService.resetDebugState();
  }
}
