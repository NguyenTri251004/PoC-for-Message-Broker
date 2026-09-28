import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { OrderController } from './order.controller';
import { OrderService } from './order.service';
import { DatabaseService } from './database.service';
import { MessageBrokerService } from './message-broker.service';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true })],
  controllers: [OrderController],
  providers: [DatabaseService, MessageBrokerService, OrderService],
})
export class AppModule {}
