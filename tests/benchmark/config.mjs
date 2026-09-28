// Shared benchmark configuration and small helpers.
//
// All values here describe the *local development* benchmark. They are read by
// the orchestrator (run-benchmark.mjs) and recorded in the final report so the
// environment is fully documented and reproducible.

import { execFileSync } from 'child_process';
import { performance } from 'perf_hooks';

export const ROOT = process.cwd();
export const RESULTS_DIR = 'tests/results/benchmark';
export const RAW_DIR = 'tests/results/benchmark/raw';

export const CONCURRENCY_LEVELS = [100, 500, 1000];
export const DURATION = process.env.BENCH_DURATION ?? '20s';
export const WARMUP_DURATION = process.env.BENCH_WARMUP ?? '5s';
export const ITERATIONS = Number(process.env.BENCH_ITERATIONS ?? 3);
export const WARMUP_VUS = Number(process.env.BENCH_WARMUP_VUS ?? 5);
// Bounded window to wait for the async backlog to drain after load stops.
export const DRAIN_TIMEOUT_MS = Number(process.env.BENCH_DRAIN_TIMEOUT_MS ?? 120000);

export const GATEWAY_URL = process.env.API_GATEWAY_URL ?? 'http://127.0.0.1:3000';
export const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL ?? 'http://127.0.0.1:3002';
export const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL ?? 'http://127.0.0.1:3003';
export const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL ?? 'http://127.0.0.1:3004';
export const ANALYTICS_SERVICE_URL = process.env.ANALYTICS_SERVICE_URL ?? 'http://127.0.0.1:3005';

export const QUEUES = [
  'payment.order.queue',
  'payment.order.queue.dlq',
  'inventory.order.queue',
  'inventory.order.queue.dlq',
  'notification.order.queue',
  'notification.order.queue.dlq',
  'analytics.order.queue',
  'analytics.order.queue.dlq',
];

export const INITIAL_STOCK = process.env.BENCH_INITIAL_STOCK ? Number(process.env.BENCH_INITIAL_STOCK) : 1000000;

// k6 runs from a container attached to the compose network so it can reach the
// gateway by container name. This keeps the load generator identical across runs.
export const K6_IMAGE = process.env.K6_IMAGE ?? 'grafana/k6:latest';

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function docker(args, options = {}) {
  return execFileSync('docker', args, {
    cwd: ROOT,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    maxBuffer: 64 * 1024 * 1024,
    ...options,
  });
}

export function dockerCompose(args, options = {}) {
  return docker(['compose', ...args], options);
}

export async function httpJson(method, url, body, options = {}) {
  const { timeoutMs = 30000, retries = 2 } = options;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const startedAt = performance.now();
    try {
      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
      const text = await response.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = text;
      }
      return { status: response.status, ok: response.ok, durationMs: Number((performance.now() - startedAt).toFixed(1)), json, text };
    } catch (error) {
      const code = error && error.cause ? error.cause.code : undefined;
      const retryable = code === 'ECONNRESET' || code === 'ECONNREFUSED' || code === 'UND_ERR_SOCKET' || (error && error.name === 'TypeError');
      if (!retryable || attempt === retries) {
        throw error;
      }
      await sleep(250 * (attempt + 1));
    } finally {
      clearTimeout(timer);
    }
  }
  throw new Error('unreachable');
}
