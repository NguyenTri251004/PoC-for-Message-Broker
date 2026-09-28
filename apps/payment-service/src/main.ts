import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  const port = Number(process.env.PORT ?? process.env.PAYMENT_SERVICE_PORT ?? 3002);
  await app.listen(port);
  console.log(`Payment Service listening on ${port}`);
}

bootstrap();
