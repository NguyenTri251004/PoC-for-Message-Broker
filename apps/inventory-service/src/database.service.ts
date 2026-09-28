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
      CREATE TABLE IF NOT EXISTS inventory_items (
        product_id VARCHAR(200) PRIMARY KEY,
        available INTEGER NOT NULL,
        reserved INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS inventory_events (
        event_id UUID PRIMARY KEY,
        order_id UUID NOT NULL,
        product_id VARCHAR(200) NOT NULL,
        quantity INTEGER NOT NULL,
        trace_id VARCHAR(120) NOT NULL,
        status VARCHAR(80) NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await this.pool.query(
      `INSERT INTO inventory_items (product_id, available, reserved)
       VALUES ($1, $2, $3)
       ON CONFLICT (product_id) DO NOTHING`,
      ['sku-001', 1000, 0],
    );
  }

  /**
   * Acquire a dedicated connection from the pool. Callers MUST release it with
   * `releaseClient` so that concurrent requests use independent connections and
   * genuinely exercise database-level concurrency guarantees.
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
