import { Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/health')
  async health() {
    return this.appService.getHealth();
  }

  @Post('/notifications/send')
  async send(@Body() payload: Record<string, unknown>) {
    return this.appService.send(payload);
  }

  @Get('/debug/sent')
  async sent() {
    return this.appService.getSent();
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
