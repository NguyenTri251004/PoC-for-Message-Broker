import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  const port = Number(process.env.PORT ?? process.env.NOTIFICATION_SERVICE_PORT ?? 3004);
  await app.listen(port);
  console.log(`Notification Service listening on ${port}`);
}

bootstrap();
