import { Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/health')
  async health() {
    return this.appService.getHealth();
  }

  @Post('/analytics/track')
  async track(@Body() payload: Record<string, unknown>) {
    return this.appService.track(payload);
  }

  @Get('/debug/tracked')
  async tracked() {
    return this.appService.getTracked();
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
