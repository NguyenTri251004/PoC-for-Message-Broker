import { Injectable, OnModuleInit } from '@nestjs/common';
import { Pool, PoolClient } from 'pg';

@Injectable()
export class DatabaseService implements OnModuleInit {
  private pool: Pool | null = null;

  async onModuleInit() {
    this.pool = new Pool({
      host: process.env.POSTGRES_HOST ?? 'localhost',
      port: Number(process.env.POSTGRES_PORT ?? 5432),
      user: process.env.POSTGRES_USER ?? 'postgres',
      password: process.env.POSTGRES_PASSWORD ?? 'postgres',
      database: process.env.POSTGRES_DB ?? 'ecommerce',
      max: Number(process.env.POSTGRES_POOL_MAX ?? 20),
    });

    await this.pool.query('SELECT 1');
    await this.ensureSchema();
  }

  private async ensureSchema() {
    if (!this.pool) {
      throw new Error('db not initialized');
    }

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id UUID PRIMARY KEY,
        customer_id VARCHAR(200) NOT NULL,
        product_id VARCHAR(200) NOT NULL,
        quantity INTEGER NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL,
        status VARCHAR(50) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS outbox_events (
        id UUID PRIMARY KEY,
        aggregate_type VARCHAR(100) NOT NULL,
        aggregate_id VARCHAR(100) NOT NULL,
        event_type VARCHAR(100) NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        published BOOLEAN NOT NULL DEFAULT FALSE
      );
    `);
  }

  /**
   * Acquire a dedicated connection from the pool. Callers MUST release it with
   * `releaseClient` so that concurrent requests use independent connections.
   */
  async getClient(): Promise<PoolClient> {
    if (!this.pool) {
      throw new Error('db not initialized');
    }
    return this.pool.connect();
  }

  releaseClient(client: PoolClient) {
    client.release();
  }
}

