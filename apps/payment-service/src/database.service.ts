import { Injectable, OnModuleInit } from '@nestjs/common';
import { Client } from 'pg';

@Injectable()
export class DatabaseService implements OnModuleInit {
  private client: Client | null = null;

  async onModuleInit() {
    this.client = new Client({
      host: process.env.POSTGRES_HOST ?? 'localhost',
      port: Number(process.env.POSTGRES_PORT ?? 5432),
      user: process.env.POSTGRES_USER ?? 'postgres',
      password: process.env.POSTGRES_PASSWORD ?? 'postgres',
      database: process.env.POSTGRES_DB ?? 'ecommerce',
    });

    await this.client.connect();
    await this.ensureSchema();
  }

  private async ensureSchema() {
    if (!this.client) {
      throw new Error('db not initialized');
    }

    await this.client.query(`
      CREATE TABLE IF NOT EXISTS payment_events (
        event_id UUID PRIMARY KEY,
        order_id UUID NOT NULL,
        trace_id VARCHAR(120) NOT NULL,
        retry_count INTEGER NOT NULL DEFAULT 0,
        status VARCHAR(80) NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_failure_reason TEXT
      );
    `);
  }

  getClient() {
    if (!this.client) {
      throw new Error('db not initialized');
    }

    return this.client;
  }
}