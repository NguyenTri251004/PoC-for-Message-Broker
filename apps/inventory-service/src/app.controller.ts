import { Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/health')
  async health() {
    return this.appService.getHealth();
  }

  @Post('/inventory/reserve')
  async reserve(@Body() payload: Record<string, unknown>) {
    return this.appService.reserve(payload);
  }

  @Get('/inventory/stock')
  async stock() {
    return this.appService.getStock();
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
