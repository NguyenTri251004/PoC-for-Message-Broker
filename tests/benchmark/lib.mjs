// Low-level helpers used by the benchmark orchestrator: database access, queue
// inspection, service reset/warm-up, and k6 execution.

import { writeFileSync, mkdirSync } from 'fs';
import {
  ROOT, RAW_DIR, QUEUES, INITIAL_STOCK,
  GATEWAY_URL, PAYMENT_SERVICE_URL, INVENTORY_SERVICE_URL, NOTIFICATION_SERVICE_URL, ANALYTICS_SERVICE_URL,
  K6_IMAGE, docker, dockerCompose, httpJson, sleep,
} from './config.mjs';

export function sql(query) {
  return dockerCompose([
    'exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'ecommerce', '-t', '-A', '-F', '|', '-c', query,
  ]).trim();
}

export function sqlRows(query, columns) {
  const raw = sql(query);
  if (!raw) {
    return [];
  }
  return raw.split('\n').map((line) => {
    const values = line.split('|');
    const row = {};
    columns.forEach((column, index) => {
      row[column] = values[index];
    });
    return row;
  });
}

export function rabbitQueues() {
  const out = dockerCompose(['exec', '-T', 'rabbitmq', 'rabbitmqctl', 'list_queues', 'name', 'messages', 'messages_ready', 'messages_unacknowledged', '--formatter', 'json']);
  try {
    return JSON.parse(out);
  } catch {
    return [];
  }
}

export function queueSnapshot(queues, names) {
  return queues
    .filter((queue) => names.includes(queue.name))
    .map((queue) => ({
      name: queue.name,
      messages: Number(queue.messages ?? 0),
      messagesReady: Number(queue.messages_ready ?? 0),
      messagesUnacknowledged: Number(queue.messages_unacknowledged ?? 0),
    }));
}

export async function resetDatabase() {
  sql('DELETE FROM orders;');
  sql('DELETE FROM payment_events;');
  sql('DELETE FROM inventory_events;');
  sql(`UPDATE inventory_items SET available = ${INITIAL_STOCK}, reserved = 0, updated_at = NOW() WHERE product_id = 'sku-001';`);
}

/**
 * Restore inventory stock to the benchmark starting level.
 *
 * NOTE: the inventory `/debug/reset` endpoint (used by the failure test suite)
 * hard-codes stock to 1000. For a high-throughput benchmark that would exhaust
 * stock almost immediately and poison the results, so the benchmark explicitly
 * re-applies a large stock level AFTER the downstream reset.
 */
export function setStock(amount = INITIAL_STOCK) {
  sql(`UPDATE inventory_items SET available = ${amount}, reserved = 0, updated_at = NOW() WHERE product_id = 'sku-001';`);
  return Number(sql("SELECT available FROM inventory_items WHERE product_id='sku-001';"));
}

/**
 * Purge every benchmark queue and its DLQ so each run starts from an empty
 * broker. Iterates over the explicit queue names (purge_queue takes a single
 * queue per invocation).
 */
export function purgeQueues() {
  for (const queue of QUEUES) {
    try {
      dockerCompose(['exec', '-T', 'rabbitmq', 'rabbitmqctl', 'purge_queue', queue]);
    } catch {
      // Queue may not exist yet on a cold start; ignore.
    }
  }
}

export function consumerCounts() {
  const out = dockerCompose(['exec', '-T', 'rabbitmq', 'rabbitmqctl', 'list_consumers', '--formatter', 'json']);
  try {
    return JSON.parse(out);
  } catch {
    return [];
  }
}

/** Fetch the in-memory processed-event counts from notification & analytics. */
export async function downstreamInMemoryCounts() {
  const read = async (url, key) => {
    try {
      const response = await httpJson('GET', url, undefined, { timeoutMs: 5000, retries: 1 });
      const json = response.json ?? {};
      const value = json[key] ?? json.count ?? (Array.isArray(json) ? json.length : undefined);
      return value;
    } catch {
      return null;
    }
  };
  return {
    notificationSent: await read(`${NOTIFICATION_SERVICE_URL}/debug/sent`, 'sent'),
    notificationQueueBacklog: null,
    analyticsTracked: await read(`${ANALYTICS_SERVICE_URL}/debug/tracked`, 'tracked'),
  };
}

export async function serviceHealth(url) {
  try {
    const response = await httpJson('GET', `${url}/health`, undefined, { timeoutMs: 5000, retries: 0 });
    return response.ok && response.json && response.json.status === 'ok';
  } catch {
    return false;
  }
}

export async function waitForStackReady(timeoutMs = 180000) {
  const services = [GATEWAY_URL, PAYMENT_SERVICE_URL, INVENTORY_SERVICE_URL, NOTIFICATION_SERVICE_URL, ANALYTICS_SERVICE_URL];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const checks = await Promise.all(services.map((url) => serviceHealth(url)));
    if (checks.every(Boolean)) {
      return true;
    }
    await sleep(2000);
  }
  return false;
}

/** Wait until every benchmark queue has an active consumer. */
export async function waitForConsumers(timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const consumers = consumerCounts();
    const queues = new Set(consumers.map((c) => c.queue && c.queue.name));
    const mainQueues = QUEUES.filter((q) => !q.endsWith('.dlq'));
    if (mainQueues.every((q) => queues.has(q))) {
      return true;
    }
    await sleep(1000);
  }
  return false;
}

export async function configureServices({ disableFailures = true } = {}) {
  if (disableFailures) {
    await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/config`, { mode: 'off', disableRandomFailures: true });
  }
  await httpJson('POST', `${INVENTORY_SERVICE_URL}/debug/config`, { mode: 'off' });
}

export async function resetDownstreamState() {
  await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/reset`, {});
  await httpJson('POST', `${INVENTORY_SERVICE_URL}/debug/reset`, {});
  await httpJson('POST', `${NOTIFICATION_SERVICE_URL}/debug/reset`, {});
  await httpJson('POST', `${ANALYTICS_SERVICE_URL}/debug/reset`, {});
}

export function networkName() {
  // e.g. poc_default
  const project = dockerCompose(['ps', '-q']).trim(); // touch compose to ensure project resolved
  void project;
  const name = docker(['inspect', 'poc-api-gateway-1', '--format', '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}']).trim();
  return name || 'poc_default';
}

/**
 * Run one k6 scenario using the grafana/k6 container attached to the compose
 * network. Returns the parsed summary export (JSON) plus the raw text.
 */
export function runK6({ mode, vus, duration, iterationLabel }) {
  mkdirSync(RAW_DIR, { recursive: true });
  const network = networkName();
  const summaryPath = `tests/results/benchmark/raw/${mode}-${vus}-${iterationLabel}.json`;

  const containerSummaryPath = '/summary.json';
  const args = [
    'run', '--rm',
    '--network', network,
    '-v', `${ROOT.replace(/\\/g, '/')}/tests/benchmark/k6:/scripts:ro`,
    '-v', `${ROOT.replace(/\\/g, '/')}/tests/results/benchmark/raw:/out`,
    '-e', `MODE=${mode}`,
    '-e', `VUS=${vus}`,
    '-e', `DURATION=${duration}`,
    '-e', 'GATEWAY_URL=http://api-gateway:3000',
    K6_IMAGE,
    'run',
    '--summary-export', `/out/${mode}-${vus}-${iterationLabel}.json`,
    '--quiet',
    '/scripts/order-flow.js',
  ];

  const raw = docker(args, { stdio: ['ignore', 'pipe', 'pipe'], encoding: 'utf8' });
  void containerSummaryPath;
  return { summaryPath, raw };
}

export function extractK6Metrics(summary) {
  // k6 summary-export format: { metrics: { metric_name: { ...trends... } } }
  const metrics = summary && summary.metrics ? summary.metrics : {};
  const pick = (name) => metrics[name] ?? null;
  const httpReq = pick('http_req_duration') ?? {};
  const reqs = pick('http_reqs') ?? {};
  const failed = pick('http_req_failed') ?? {};
  const outcomes = pick('order_outcomes') ?? {};

  return {
    totalRequests: Number(reqs.count ?? 0),
    rps: Number(reqs.rate ?? 0),
    errorRate: Number(failed.rate ?? 0),
    p50: Number(httpReq['p(50)'] ?? httpReq.med ?? 0),
    p95: Number(httpReq['p(95)'] ?? 0),
    p99: Number(httpReq['p(99)'] ?? 0),
    min: Number(httpReq.min ?? 0),
    max: Number(httpReq.max ?? 0),
    avg: Number(httpReq.avg ?? 0),
    successCount: outcomes.values ? Number(outcomes.values.success ?? 0) : undefined,
    failureCount: outcomes.values ? Number(outcomes.values.failure ?? 0) : undefined,
  };
}

export function writeJson(path, data) {
  mkdirSync(RAW_DIR, { recursive: true });
  writeFileSync(path, JSON.stringify(data, null, 2), 'utf8');
}
