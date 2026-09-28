import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  const port = Number(process.env.PORT ?? process.env.ANALYTICS_SERVICE_PORT ?? 3005);
  await app.listen(port);
  console.log(`Analytics Service listening on ${port}`);
}

bootstrap();
