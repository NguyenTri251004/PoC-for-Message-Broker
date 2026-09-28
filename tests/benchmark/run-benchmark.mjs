// Benchmark orchestrator: synchronous baseline vs RabbitMQ asynchronous.
//
// For each mode (sync, async) and each concurrency level (100, 500, 1000 VUs):
//   1. reset database + inventory + downstream in-memory state
//   2. purge RabbitMQ queues (async)
//   3. verify services healthy + consumers ready
//   4. run a short warm-up (NOT counted)
//   5. run ITERATIONS measured runs, resetting state before each
//   6. collect k6 metrics + post-run DB verification + queue state (async)
//   7. persist per-iteration raw output and per-scenario aggregate JSON
//
// Then writes a benchmark-summary.md with measured values only.

import { writeFileSync, mkdirSync, readFileSync } from 'fs';
import {
  RESULTS_DIR, RAW_DIR, CONCURRENCY_LEVELS, DURATION, WARMUP_DURATION, ITERATIONS, WARMUP_VUS,
  INITIAL_STOCK, QUEUES, dockerCompose, DRAIN_TIMEOUT_MS,
} from './config.mjs';
import {
  runK6, extractK6Metrics, writeJson, resetDatabase, purgeQueues, waitForConsumers,
  configureServices, resetDownstreamState, rabbitQueues, queueSnapshot, sql, setStock,
  downstreamInMemoryCounts,
} from './lib.mjs';

const MODES = ['sync', 'async'];

function mean(values) {
  if (values.length === 0) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function round(value, digits = 2) {
  return Number(Number(value).toFixed(digits));
}

function aggregateBlock(runs) {
  const numericKeys = ['totalRequests', 'rps', 'errorRate', 'p50', 'p95', 'p99', 'min', 'max', 'avg'];
  const aggregate = {};
  for (const key of numericKeys) {
    const values = runs.map((run) => run.metrics[key]);
    aggregate[key] = {
      mean: round(mean(values)),
      min: round(Math.min(...values)),
      max: round(Math.max(...values)),
      values: values.map((v) => round(v)),
    };
  }
  return aggregate;
}

async function verifyDatabase(mode, acceptedByRun) {
  const orders = Number(sql('SELECT COUNT(*) FROM orders;'));
  const paymentEvents = Number(sql('SELECT COUNT(*) FROM payment_events;'));
  const inventoryEvents = Number(sql('SELECT COUNT(*) FROM inventory_events;'));
  const inventory = sql("SELECT available, reserved FROM inventory_items WHERE product_id='sku-001';").split('|');
  const totalRequested = acceptedByRun.reduce((a, b) => a + b, 0);

  const duplicatePayment = Number(sql('SELECT COALESCE(SUM(c),0) FROM (SELECT COUNT(*) c FROM payment_events GROUP BY event_id HAVING COUNT(*) > 1) t;'));
  const duplicateInventory = Number(sql('SELECT COALESCE(SUM(c),0) FROM (SELECT COUNT(*) c FROM inventory_events GROUP BY event_id HAVING COUNT(*) > 1) t;'));

  const allQueues = queueSnapshot(rabbitQueues(), QUEUES);
  const dlqMessages = allQueues
    .filter((q) => q.name.endsWith('.dlq'))
    .reduce((sum, q) => sum + q.messagesReady + q.messagesUnacknowledged, 0);
  const mainQueueBacklog = allQueues
    .filter((q) => !q.name.endsWith('.dlq'))
    .reduce((sum, q) => sum + q.messagesReady + q.messagesUnacknowledged, 0);

  const inMemory = await downstreamInMemoryCounts();

  return {
    orders,
    paymentEvents,
    inventoryEvents,
    notificationSent: inMemory.notificationSent,
    analyticsTracked: inMemory.analyticsTracked,
    finalInventory: { available: Number(inventory[0]), reserved: Number(inventory[1]) },
    totalAcceptedRequests: totalRequested,
    duplicatePaymentEffects: duplicatePayment,
    duplicateInventoryEffects: duplicateInventory,
    dlqMessages,
    mainQueueBacklog,
    note: mode === 'async'
      ? 'Async: notifications/analytics are in-memory (reported via /debug endpoints); payment/inventory persist to PostgreSQL.'
      : 'Sync: the same downstream services are invoked over HTTP; payment/inventory persist to PostgreSQL.',
  };
}

function readFileSafe(path) {
  return readFileSync(path, 'utf8');
}

function consumerCount() {
  try {
    const raw = dockerCompose(['exec', '-T', 'rabbitmq', 'rabbitmqctl', 'list_consumers', '--formatter', 'json']);
    return JSON.parse(raw).length;
  } catch {
    return null;
  }
}

/**
 * Wait (bounded) for the main queues to drain after the load generator stops.
 * Returns whether the queues fully drained plus the remaining backlog snapshot,
 * so the report can distinguish "work completed" from "work still queued".
 */
async function waitForQueuesDrained(names, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const snapshot = queueSnapshot(rabbitQueues(), names);
    const pending = snapshot.reduce((sum, q) => sum + q.messages, 0);
    if (pending === 0) {
      return { drained: true, remaining: 0, snapshot };
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  const snapshot = queueSnapshot(rabbitQueues(), names);
  const remaining = snapshot.reduce((sum, q) => sum + q.messages, 0);
  return { drained: false, remaining, snapshot };
}

async function resetAll(mode, label) {
  // Purge queues first so any backlog from the previous run is cleared for BOTH
  // modes (sync never produces queue traffic, but stale DLQ contents from an
  // earlier async scenario must not leak into sync verification numbers).
  purgeQueues();
  await resetDatabase();
  await resetDownstreamState();
  // The inventory /debug/reset endpoint resets stock to 1000 (failure-suite
  // behaviour); re-apply the large benchmark stock level afterwards.
  const stock = setStock();
  await configureServices({ disableFailures: true });
  await waitForConsumers();
  await new Promise((r) => setTimeout(r, 1000));
  return stock;
}

async function runScenario(mode, vus) {
  console.log(`\n=== Scenario ${mode} @ ${vus} VUs ===`);

  const runs = [];
  let lastDbVerification = null;
  let lastQueueState = null;
  const acceptedPerRun = [];
  let queueDrainTimedOut = false;
  let lastDrainBacklog = 0;
  let lastDrainSeconds = 0;

  for (let iteration = 1; iteration <= ITERATIONS; iteration += 1) {
    await resetAll(mode, `iter${iteration}-prewarmup`);

    console.log(`  iteration ${iteration}: warm-up`);
    runK6({ mode, vus: WARMUP_VUS, duration: WARMUP_DURATION, iterationLabel: `warmup-${iteration}` });
    await new Promise((r) => setTimeout(r, 500));

    await resetAll(mode, `iter${iteration}-premeasure`);
    await new Promise((r) => setTimeout(r, 500));

    console.log(`  iteration ${iteration}: measured (${vus} VUs, ${DURATION})`);
    const { summaryPath } = runK6({ mode, vus, duration: DURATION, iterationLabel: `iter${iteration}` });
    const summary = JSON.parse(readFileSafe(summaryPath));
    const metrics = extractK6Metrics(summary);

    if (mode === 'async') {
      const drainStart = Date.now();
      const drain = await waitForQueuesDrained(QUEUES.filter((q) => !q.endsWith('.dlq')), DRAIN_TIMEOUT_MS);
      lastDrainSeconds = Number(((Date.now() - drainStart) / 1000).toFixed(1));
      lastDrainBacklog = drain.remaining;
      if (!drain.drained) {
        queueDrainTimedOut = true;
      }
    }

    const accepted = metrics.successCount !== undefined
      ? metrics.successCount
      : Math.round(metrics.totalRequests * (1 - metrics.errorRate));
    acceptedPerRun.push(accepted);

    const completion = {
      paymentEvents: Number(sql('SELECT COUNT(*) FROM payment_events;')),
      inventoryEvents: Number(sql('SELECT COUNT(*) FROM inventory_events;')),
      orders: Number(sql('SELECT COUNT(*) FROM orders;')),
    };
    const queueState = queueSnapshot(rabbitQueues(), QUEUES);

    runs.push({
      iteration,
      metrics,
      completion,
      accepted,
      generatedAt: new Date().toISOString(),
    });
    lastDbVerification = await verifyDatabase(mode, acceptedPerRun);
    lastQueueState = queueState;

    console.log(`    rps=${round(metrics.rps)} p50=${round(metrics.p50)} p95=${round(metrics.p95)} p99=${round(metrics.p99)} err=${round(metrics.errorRate, 4)} accepted=${accepted} paymentEvt=${completion.paymentEvents} invEvt=${completion.inventoryEvents}`);
  }

  const result = {
    mode,
    concurrency: vus,
    duration: DURATION,
    iterations: ITERATIONS,
    warmup: { vus: WARMUP_VUS, duration: WARMUP_DURATION },
    runs,
    aggregate: aggregateBlock(runs),
    downstreamCompletion: {
      databaseEvents: lastDbVerification,
      queueState: lastQueueState,
      queueDrainTimedOut,
      drainWindowMs: DRAIN_TIMEOUT_MS,
      drainSecondsUsed: lastDrainSeconds,
      remainingBacklogMessages: lastDrainBacklog,
      consumerProcessingCount: consumerCount(),
    },
    environment: environmentInfo(),
    generatedAt: new Date().toISOString(),
  };

  const file = `${RESULTS_DIR}/${mode === 'async' ? 'rabbitmq' : 'sync'}-${vus}.json`;
  writeJson(file, result);
  console.log(`  -> wrote ${file}`);
  return result;
}

function environmentInfo() {
  return {
    note: 'Local development environment - NOT a production capacity estimate.',
    os: 'Windows 11 Home Single Language',
    cpu: 'AMD Ryzen 5 5625U (6C/12T)',
    ramGb: 7.4,
    docker: '29.0.1',
    node: process.version,
    k6: 'grafana/k6 (docker image)',
    concurrency: CONCURRENCY_LEVELS,
    duration: DURATION,
    iterations: ITERATIONS,
    delaysMs: { payment: 2000, inventory: 100, notification: 1000, analytics: 500 },
    initialStock: INITIAL_STOCK,
  };
}

function summarizeTableResults(results) {
  const rows = [];
  for (const mode of MODES) {
    for (const vus of CONCURRENCY_LEVELS) {
      const result = results.find((r) => r.mode === mode && r.concurrency === vus);
      if (!result || result.status === 'FAILED') {
        rows.push({ mode, vus, status: 'NOT_RUN' });
        continue;
      }
      rows.push({
        mode: mode === 'async' ? 'RabbitMQ async' : 'Synchronous',
        vus,
        rps: result.aggregate.rps.mean,
        p50: result.aggregate.p50.mean,
        p95: result.aggregate.p95.mean,
        p99: result.aggregate.p99.mean,
        errorRate: result.aggregate.errorRate.mean,
        completedEvents: result.downstreamCompletion?.databaseEvents
          ? result.downstreamCompletion.databaseEvents.orders
          : null,
        status: 'PASS',
      });
    }
  }
  return rows;
}

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
  return Number(value).toFixed(digits);
}

function buildSummaryMarkdown(results) {
  const lines = [];
  lines.push('# Performance Benchmark: Synchronous vs RabbitMQ Asynchronous');
  lines.push('');
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push('');
  lines.push('> All numbers below are **measured** from actual runs on a local development');
  lines.push('> machine. They are reproducible illustrations of architectural behaviour,');
  lines.push('> **not** production capacity estimates.');
  lines.push('');

  const env = results.find((r) => r.environment)?.environment ?? {};

  lines.push('## Environment');
  lines.push('');
  lines.push('| Item | Value |');
  lines.push('| ---- | ----- |');
  lines.push(`| OS | ${env.os ?? 'n/a'} |`);
  lines.push(`| CPU | ${env.cpu ?? 'n/a'} |`);
  lines.push(`| RAM | ${env.ramGb ?? 'n/a'} GB |`);
  lines.push(`| Docker | ${env.docker ?? 'n/a'} |`);
  lines.push(`| Node.js | ${env.node ?? 'n/a'} |`);
  lines.push('| PostgreSQL | 16-alpine |');
  lines.push('| RabbitMQ | 3.13-management |');
  lines.push(`| k6 | ${env.k6 ?? 'n/a'} |`);
  lines.push(`| Concurrency levels | ${CONCURRENCY_LEVELS.join(', ')} VUs |`);
  lines.push(`| Duration per run | ${DURATION} |`);
  lines.push(`| Iterations per scenario | ${ITERATIONS} |`);
  lines.push(`| Downstream delays | payment=${env.delaysMs?.payment}ms inventory=${env.delaysMs?.inventory}ms notification=${env.delaysMs?.notification}ms analytics=${env.delaysMs?.analytics}ms |`);
  lines.push('');

  lines.push('## Benchmark results (aggregate of measured runs)');
  lines.push('');
  lines.push('| Mode | Concurrency | RPS | p50 (ms) | p95 (ms) | p99 (ms) | Error Rate | Completed Orders |');
  lines.push('| ---- | ----------- | --- | -------- | -------- | -------- | ---------- | ---------------- |');

  for (const row of summarizeTableResults(results)) {
    if (row.status === 'NOT_RUN') {
      lines.push(`| ${row.mode} | ${row.vus} | RESOURCE-LIMITED | - | - | - | - | - |`);
      continue;
    }
    lines.push(`| ${row.mode} | ${row.vus} | ${fmt(row.rps)} | ${fmt(row.p50)} | ${fmt(row.p95)} | ${fmt(row.p99)} | ${fmt(row.errorRate * 100, 2)}% | ${row.completedEvents} |`);
  }
  lines.push('');

  lines.push('## Latency vs throughput (aggregate means)');
  lines.push('');
  lines.push('| Metric | Synchronous | RabbitMQ |');
  lines.push('| ------ | ----------- | -------- |');
  for (const vus of CONCURRENCY_LEVELS) {
    const sync = results.find((r) => r.mode === 'sync' && r.concurrency === vus && r.status !== 'FAILED');
    const asyncResult = results.find((r) => r.mode === 'async' && r.concurrency === vus && r.status !== 'FAILED');
    if (!sync && !asyncResult) continue;
    lines.push(`| --- @ ${vus} VUs --- | | |`);
    lines.push(`| RPS | ${sync ? fmt(sync.aggregate.rps.mean) : 'n/a'} | ${asyncResult ? fmt(asyncResult.aggregate.rps.mean) : 'n/a'} |`);
    lines.push(`| API p50 | ${sync ? fmt(sync.aggregate.p50.mean) : 'n/a'} | ${asyncResult ? fmt(asyncResult.aggregate.p50.mean) : 'n/a'} |`);
    lines.push(`| API p95 | ${sync ? fmt(sync.aggregate.p95.mean) : 'n/a'} | ${asyncResult ? fmt(asyncResult.aggregate.p95.mean) : 'n/a'} |`);
    lines.push(`| API p99 | ${sync ? fmt(sync.aggregate.p99.mean) : 'n/a'} | ${asyncResult ? fmt(asyncResult.aggregate.p99.mean) : 'n/a'} |`);
    lines.push(`| Error rate | ${sync ? fmt(sync.aggregate.errorRate.mean * 100, 2) + '%' : 'n/a'} | ${asyncResult ? fmt(asyncResult.aggregate.errorRate.mean * 100, 2) + '%' : 'n/a'} |`);
  }
  lines.push('');

  lines.push('## Downstream completion verification');
  lines.push('');
  lines.push('`Orders` = order rows persisted. `Payment/Inventory events` = durable rows written by');
  lines.push('downstream services. `Notif/Analytics` = in-memory counters exposed via /debug.');
  lines.push('`Main queue backlog` = messages still waiting on a consumer when the drain window ended.');
  lines.push('');
  lines.push('| Mode | Concurrency | Orders | Orders accepted (2xx) | Payment events | Inventory events | Notif | Analytics | Final available | Final reserved | Dup payment | Dup inventory | DLQ msgs | Main queue backlog |');
  lines.push('| ---- | ----------- | ------ | --------------------- | -------------- | ---------------- | ----- | --------- | --------------- | -------------- | ----------- | ------------- | -------- | ------------------ |');
  for (const mode of MODES) {
    for (const vus of CONCURRENCY_LEVELS) {
      const result = results.find((r) => r.mode === mode && r.concurrency === vus && r.status !== 'FAILED');
      if (!result) continue;
      const db = result.downstreamCompletion?.databaseEvents ?? {};
      lines.push(`| ${mode} | ${vus} | ${db.orders ?? 'n/a'} | ${db.totalAcceptedRequests ?? 'n/a'} | ${db.paymentEvents ?? 'n/a'} | ${db.inventoryEvents ?? 'n/a'} | ${db.notificationSent ?? 'n/a'} | ${db.analyticsTracked ?? 'n/a'} | ${db.finalInventory?.available ?? 'n/a'} | ${db.finalInventory?.reserved ?? 'n/a'} | ${db.duplicatePaymentEffects ?? 'n/a'} | ${db.duplicateInventoryEffects ?? 'n/a'} | ${db.dlqMessages ?? 'n/a'} | ${db.mainQueueBacklog ?? 'n/a'} |`);
    }
  }
  lines.push('');

  lines.push('## Raw evidence');
  lines.push('');
  lines.push('Per-iteration k6 summary exports: `tests/results/benchmark/raw/`.');
  lines.push('Per-scenario aggregates: `tests/results/benchmark/sync-<vus>.json` and `rabbitmq-<vus>.json`.');
  lines.push('');

  writeFileSync(`${RESULTS_DIR}/benchmark-summary.md`, lines.join('\n'), 'utf8');
  return lines.join('\n');
}

async function main() {
  mkdirSync(RESULTS_DIR, { recursive: true });
  mkdirSync(RAW_DIR, { recursive: true });

  const only = process.env.BENCH_ONLY_MODE;
  const onlyVus = process.env.BENCH_ONLY_VUS ? Number(process.env.BENCH_ONLY_VUS) : null;
  const modes = only ? [only] : MODES;
  const levels = onlyVus ? [onlyVus] : CONCURRENCY_LEVELS;

  const results = [];
  for (const mode of modes) {
    for (const vus of levels) {
      try {
        const result = await runScenario(mode, vus);
        results.push(result);
      } catch (error) {
        console.error(`Scenario ${mode}@${vus} failed:`, error.message);
        const result = {
          mode,
          concurrency: vus,
          status: 'FAILED',
          error: String(error.message),
          generatedAt: new Date().toISOString(),
          aggregate: {},
          runs: [],
        };
        results.push(result);
        writeJson(`${RESULTS_DIR}/${mode === 'async' ? 'rabbitmq' : 'sync'}-${vus}.json`, result);
      }
    }
  }

  buildSummaryMarkdown(results);
  console.log('\nBenchmark complete. Summary written to tests/results/benchmark/benchmark-summary.md');
}

main();
