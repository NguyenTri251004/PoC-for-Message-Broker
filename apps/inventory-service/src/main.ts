import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  const port = Number(process.env.PORT ?? process.env.INVENTORY_SERVICE_PORT ?? 3003);
  await app.listen(port);
  console.log(`Inventory Service listening on ${port}`);
}

bootstrap();
