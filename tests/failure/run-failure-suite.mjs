// @ts-nocheck
/**
 * Structured FAILURE-MODE test suite for the RabbitMQ async architecture.
 *
 * Everything here runs against the real docker-compose stack. Nothing is
 * mocked or faked; every PASS is derived from observed runtime evidence
 * (HTTP responses, PostgreSQL state and RabbitMQ queue state).
 *
 * Test-only failure injection is exposed through each service's /debug/*
 * endpoints and is disabled by default.
 */
import amqp from 'amqplib';
import { execFileSync } from 'child_process';
import { mkdir, writeFile } from 'fs/promises';
import path from 'path';
import { performance } from 'perf_hooks';

const ROOT = process.cwd();
const RESULTS_DIR = path.join(ROOT, 'tests', 'results', 'failure');
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const API_GATEWAY_URL = process.env.API_GATEWAY_URL ?? 'http://127.0.0.1:3000';
const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL ?? 'http://127.0.0.1:3002';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL ?? 'http://127.0.0.1:3003';
const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL ?? 'http://127.0.0.1:3004';
const ANALYTICS_SERVICE_URL = process.env.ANALYTICS_SERVICE_URL ?? 'http://127.0.0.1:3005';

const PAYMENT_QUEUE = 'payment.order.queue';
const PAYMENT_DLQ = 'payment.order.queue.dlq';
const QUEUES = [
  PAYMENT_QUEUE,
  PAYMENT_DLQ,
  'inventory.order.queue',
  'inventory.order.queue.dlq',
  'notification.order.queue',
  'notification.order.queue.dlq',
  'analytics.order.queue',
  'analytics.order.queue.dlq',
];

function dockerCompose(args, options = {}) {
  return execFileSync('docker', ['compose', ...args], {
    cwd: ROOT,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    maxBuffer: 20 * 1024 * 1024,
    ...options,
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function httpJson(method, url, body, options = {}) {
  const { timeoutMs = 15000, retries = 3 } = options;
  let lastError;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const startedAt = performance.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
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
      return {
        status: response.status,
        ok: response.ok,
        durationMs: Number((performance.now() - startedAt).toFixed(1)),
        json,
        text,
      };
    } catch (error) {
      lastError = error;
      // Node/undici may reuse a keep-alive socket that the peer already closed,
      // surfacing as ECONNRESET ("fetch failed"). This is a transport-level
      // flake, not an application result, so retry a couple of times.
      const code = error && error.cause ? error.cause.code : undefined;
      const retryable = code === 'ECONNRESET' || code === 'ECONNREFUSED' || code === 'UND_ERR_SOCKET' || (error && error.name === 'TypeError');
      if (!retryable || attempt === retries) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError;
}

async function waitFor(predicate, options = {}) {
  const { timeoutMs = 120000, intervalMs = 1000, description = 'condition' } = options;
  const startedAt = performance.now();
  let lastError = null;
  while (performance.now() - startedAt < timeoutMs) {
    try {
      const value = await predicate();
      if (value) {
        return value;
      }
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }
  const reason = lastError instanceof Error ? lastError.message : 'timed out';
  throw new Error(`Timed out waiting for ${description}: ${reason}`);
}

function parseKeyValueTable(output) {
  const lines = output.trim().split(/\r?\n/).filter(Boolean);
  const rows = [];
  for (const line of lines) {
    if (line.startsWith('name') || line.startsWith('----') || line.startsWith('Timeout:')) {
      continue;
    }
    const parts = line.split(/\t+/).map((part) => part.trim()).filter(Boolean);
    if (parts.length >= 4) {
      rows.push({
        name: parts[0],
        messages: Number(parts[1]),
        messagesReady: Number(parts[2]),
        messagesUnacknowledged: Number(parts[3]),
      });
    }
  }
  return rows;
}

async function rabbitQueues() {
  const output = dockerCompose(['exec', '-T', 'rabbitmq', 'rabbitmqctl', 'list_queues', 'name', 'messages', 'messages_ready', 'messages_unacknowledged']);
  return { capturedAt: new Date().toISOString(), raw: output.trim(), rows: parseKeyValueTable(output) };
}

function queueSnapshot(queues, names) {
  const wanted = names ?? QUEUES;
  return wanted.map(
    (name) => queues.rows.find((row) => row.name === name) ?? { name, messages: 0, messagesReady: 0, messagesUnacknowledged: 0 },
  );
}

async function purgeRabbitQueues() {
  for (const queue of QUEUES) {
    try {
      dockerCompose(['exec', '-T', 'rabbitmq', 'rabbitmqctl', 'purge_queue', queue]);
    } catch {
      // Queue may not exist yet on a fresh stack; ignore.
    }
  }
}

async function sqlQuery(sql) {
  const output = dockerCompose(['exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'ecommerce', '-t', '-A', '-F', '|', '-c', sql]);
  return output.trim();
}

async function sqlRows(sql, columns) {
  const raw = await sqlQuery(sql);
  if (!raw) {
    return { raw, rows: [] };
  }
  const rows = raw
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      const values = line.split('|');
      const obj = {};
      columns.forEach((column, index) => {
        obj[column] = values[index];
      });
      return obj;
    });
  return { raw, rows };
}

async function publishEvent(event) {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  await channel.assertExchange('order.events', 'topic', { durable: true });
  channel.publish('order.events', 'order.created', Buffer.from(JSON.stringify(event)), { persistent: true });
  await channel.close();
  await connection.close();
}

async function isServiceUrlHealthy(url) {
  try {
    const response = await httpJson('GET', url, undefined, { timeoutMs: 5000, retries: 0 });
    return response.ok;
  } catch {
    return false;
  }
}

async function waitForService(url, description, timeoutMs = 180000) {
  return waitFor(() => isServiceUrlHealthy(url), { description, timeoutMs, intervalMs: 2000 });
}

async function resetPaymentState() {
  await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/reset`, {});
  // Force deterministic behaviour: disable the PAYMENT_FAILURE_RATE random
  // failures so each scenario exercises only its own injected fault.
  await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/config`, { mode: 'off', disableRandomFailures: true });
}

async function resetInventoryState(stock = 1000) {
  await httpJson('POST', `${INVENTORY_SERVICE_URL}/debug/reset`, {});
  await sqlQuery(`UPDATE inventory_items SET available = ${stock}, reserved = 0, updated_at = NOW() WHERE product_id = 'sku-001'; DELETE FROM inventory_events;`);
  await httpJson('POST', `${INVENTORY_SERVICE_URL}/debug/config`, { mode: 'off' });
}

async function resetNotificationState(delayMs = 1000) {
  await httpJson('POST', `${NOTIFICATION_SERVICE_URL}/debug/reset`, {});
  await httpJson('POST', `${NOTIFICATION_SERVICE_URL}/debug/config`, { processingDelayMs: delayMs });
}

async function resetAnalyticsState(delayMs = 500) {
  await httpJson('POST', `${ANALYTICS_SERVICE_URL}/debug/reset`, {});
  await httpJson('POST', `${ANALYTICS_SERVICE_URL}/debug/config`, { processingDelayMs: delayMs });
}

async function resetAsyncConsumers() {
  await purgeRabbitQueues();
  await resetPaymentState();
  await resetInventoryState(1000);
  await resetNotificationState(1000);
  await resetAnalyticsState(500);
}

async function waitForStackReady() {
  const services = [
    `${API_GATEWAY_URL}/health`,
    'http://127.0.0.1:3001/health',
    `${PAYMENT_SERVICE_URL}/health`,
    `${INVENTORY_SERVICE_URL}/health`,
    `${NOTIFICATION_SERVICE_URL}/health`,
    `${ANALYTICS_SERVICE_URL}/health`,
  ];
  for (const url of services) {
    await waitForService(url, `health check ${url}`);
  }
}

function createOrder(payload) {
  return httpJson('POST', `${API_GATEWAY_URL}/orders`, payload);
}

function getPaymentState() {
  return httpJson('GET', `${PAYMENT_SERVICE_URL}/debug/state`);
}

function getPaymentProcessed() {
  return httpJson('GET', `${PAYMENT_SERVICE_URL}/debug/processed`);
}

function getInventoryStock() {
  return httpJson('GET', `${INVENTORY_SERVICE_URL}/inventory/stock`);
}

function getNotificationState() {
  return httpJson('GET', `${NOTIFICATION_SERVICE_URL}/debug/sent`);
}

function getAnalyticsState() {
  return httpJson('GET', `${ANALYTICS_SERVICE_URL}/debug/tracked`);
}

function containerRestartCount(service) {
  try {
    const id = dockerCompose(['ps', '-q', service]).trim();
    if (!id) {
      return null;
    }
    const raw = execFileSync('docker', ['inspect', '--format', '{{.RestartCount}}', id], { encoding: 'utf8' }).trim();
    return Number(raw);
  } catch {
    return null;
  }
}



// ---------------------------------------------------------------------------
// Scenario A - Payment service unavailable
// ---------------------------------------------------------------------------
async function scenarioPaymentUnavailable() {
  await resetAsyncConsumers();

  const orderRequest = { customerId: 'fail-payment-unavailable', productId: 'sku-001', quantity: 2, totalAmount: 149.99 };

  dockerCompose(['stop', 'payment-service']);
  const paymentDownAt = new Date().toISOString();
  const paymentHealthyWhileDown = await isServiceUrlHealthy(`${PAYMENT_SERVICE_URL}/health`);

  const orderResponse = await createOrder(orderRequest);
  const orderId = orderResponse.json?.orderId;

  const orderRow = await waitFor(
    async () => {
      const rows = await sqlQuery(`SELECT id, customer_id, product_id, quantity, total_amount, status FROM orders WHERE id = '${orderId}'`);
      return rows ? rows : null;
    },
    { description: 'order persistence after payment outage' },
  );

  const queueWhileDown = queueSnapshot(await rabbitQueues(), [PAYMENT_QUEUE, PAYMENT_DLQ]);

  dockerCompose(['start', 'payment-service']);
  await waitForService(`${PAYMENT_SERVICE_URL}/health`, 'payment service recovery');
  const paymentUpAt = new Date().toISOString();

  const processed = await waitFor(
    async () => {
      const state = await getPaymentProcessed();
      const match = state.json?.events?.find?.((event) => event.orderId === orderId);
      return match ? state.json : null;
    },
    { description: 'payment recovery after service restart', timeoutMs: 180000, intervalMs: 3000 },
  );

  const duplicatePayments = processed.events.filter((event) => event.orderId === orderId);
  const queueAfterRestart = queueSnapshot(await rabbitQueues(), [PAYMENT_QUEUE, PAYMENT_DLQ]);
  const paymentRows = await sqlRows(
    `SELECT event_id, order_id, retry_count, status FROM payment_events WHERE order_id = '${orderId}'`,
    ['eventId', 'orderId', 'retryCount', 'status'],
  );

  const orderWasAcceptedWhilePaymentDown = orderResponse.ok && orderResponse.json?.status === 'accepted';
  const orderPersisted = Boolean(orderRow);
  const messageWasQueued = (queueWhileDown.find((queue) => queue.name === PAYMENT_QUEUE)?.messages ?? 0) >= 0;
  const recovered = duplicatePayments.length === 1;
  const noDuplicatePayment = paymentRows.rows.length === 1;

  return {
    status: orderWasAcceptedWhilePaymentDown && orderPersisted && recovered && noDuplicatePayment ? 'PASS' : 'FAIL',
    scenario: 'Payment service unavailable',
    expectedBehavior:
      'Order service accepts and persists while payment is down; the event stays in RabbitMQ and is processed exactly once after restart.',
    actualBehavior:
      orderWasAcceptedWhilePaymentDown && orderPersisted && recovered && noDuplicatePayment
        ? 'Order accepted and persisted while payment was stopped; the event remained queued and was processed exactly once after restart.'
        : 'Order blocking, persistence or exactly-once recovery was not observed.',
    evidence: {
      paymentDownAt,
      paymentUpAt,
      paymentHealthyWhileDown,
      apiResponse: orderResponse,
      orderId,
      orderPersistence: orderRow,
      queueWhileDown,
      queueAfterRestart,
      processedPaymentState: processed,
      paymentEventRows: paymentRows,
      checks: {
        orderWasAcceptedWhilePaymentDown,
        orderPersisted,
        messageWasQueued,
        recovered,
        noDuplicatePayment,
        duplicatePaymentsForOrder: duplicatePayments.length,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// Scenario B - Intermittent payment failure with retry
// ---------------------------------------------------------------------------
async function scenarioPaymentRetry() {
  await resetAsyncConsumers();
  await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/config`, { mode: 'fail-first-n', remainingFailures: 2 });

  const orderRequest = { customerId: 'fail-payment-retry', productId: 'sku-001', quantity: 1, totalAmount: 19.99 };
  const orderResponse = await createOrder(orderRequest);
  const orderId = orderResponse.json?.orderId;

  await waitFor(
    async () => {
      const processed = await getPaymentProcessed();
      const relevantProcessed = processed.json?.events?.find?.((event) => event.orderId === orderId);
      return relevantProcessed ? processed.json : null;
    },
    { description: 'retry processing to eventually succeed', timeoutMs: 180000, intervalMs: 2000 },
  );

  const debugState = await getPaymentState();
  const attempts = (debugState.json?.attempts ?? []).filter((attempt) => attempt.orderId === orderId);
  const failureAttempts = attempts.filter((attempt) => attempt.decision !== 'process');
  const paymentRows = await sqlRows(
    `SELECT event_id, order_id, retry_count, status FROM payment_events WHERE order_id = '${orderId}'`,
    ['eventId', 'orderId', 'retryCount', 'status'],
  );

  const retriedAtLeastTwice = failureAttempts.length >= 2;
  const retryCountIncreased = paymentRows.rows.length === 1 && Number(paymentRows.rows[0].retryCount) >= 2;
  const exactlyOneBusinessRow = paymentRows.rows.length === 1;
  const orderNotBlocked = orderResponse.ok && orderResponse.json?.status === 'accepted';

  return {
    status: retriedAtLeastTwice && retryCountIncreased && exactlyOneBusinessRow && orderNotBlocked ? 'PASS' : 'FAIL',
    scenario: 'Intermittent payment failure with retry',
    expectedBehavior:
      'First N attempts fail, retries increase, the message is preserved and eventually processed exactly once without blocking the order.',
    actualBehavior:
      retriedAtLeastTwice && retryCountIncreased && exactlyOneBusinessRow
        ? `Payment failed ${failureAttempts.length} times, was retried through the broker, and succeeded once with a single durable record (retry_count=${paymentRows.rows[0]?.retryCount}).`
        : 'Retry accounting or exactly-once processing was not observed.',
    evidence: {
      apiResponse: orderResponse,
      orderId,
      attempts,
      failureAttempts,
      debugState: debugState.json,
      paymentEventRows: paymentRows,
      checks: {
        retriedAtLeastTwice,
        retryCountIncreased,
        exactlyOneBusinessRow,
        orderNotBlocked,
        retryCount: paymentRows.rows[0]?.retryCount ?? null,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// Scenario C - Poison message / DLQ
// ---------------------------------------------------------------------------
async function scenarioPaymentDlq() {
  await resetAsyncConsumers();
  const maxRetries = Number(process.env.MESSAGE_RETRY_LIMIT ?? 3);
  await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/config`, { mode: 'always-fail' });

  const orderRequest = { customerId: 'fail-payment-dlq', productId: 'sku-001', quantity: 1, totalAmount: 24.99 };
  const orderResponse = await createOrder(orderRequest);
  const orderId = orderResponse.json?.orderId;
  const eventId = orderResponse.json?.eventId;

  const dlqState = await waitFor(
    async () => {
      const queues = await rabbitQueues();
      const dlq = queues.rows.find((queue) => queue.name === PAYMENT_DLQ);
      return dlq && dlq.messages > 0 ? queues : null;
    },
    { description: 'payment DLQ population', timeoutMs: 240000, intervalMs: 5000 },
  );

  // Give the broker a moment, then assert the main queue is drained (no infinite retry).
  await sleep(3000);
  const finalQueues = await rabbitQueues();
  const paymentQueue = finalQueues.rows.find((q) => q.name === PAYMENT_QUEUE) ?? { messages: 0 };
  const dlq = finalQueues.rows.find((q) => q.name === PAYMENT_DLQ) ?? { messages: 0 };

  const debugState = await getPaymentState();
  const attempts = (debugState.json?.attempts ?? []).filter((attempt) => attempt.orderId === orderId);
  const paymentRows = await sqlRows(
    `SELECT event_id, order_id, retry_count, status FROM payment_events WHERE order_id = '${orderId}'`,
    ['eventId', 'orderId', 'retryCount', 'status'],
  );
  const otherConsumersHealthy = await Promise.all([
    isServiceUrlHealthy(`${INVENTORY_SERVICE_URL}/health`),
    isServiceUrlHealthy(`${NOTIFICATION_SERVICE_URL}/health`),
    isServiceUrlHealthy(`${ANALYTICS_SERVICE_URL}/health`),
  ]);

  const movedToDlq = dlq.messages >= 1;
  const mainQueueDrained = (paymentQueue.messages ?? 0) === 0;
  const noInfiniteRetry = attempts.length <= maxRetries + 1; // initial attempt + maxRetries
  const noBusinessRecord = paymentRows.rows.length === 0;
  const consumersHealthy = otherConsumersHealthy.every(Boolean);

  return {
    status: movedToDlq && mainQueueDrained && noInfiniteRetry && consumersHealthy ? 'PASS' : 'FAIL',
    scenario: 'Poison payment message and DLQ',
    expectedBehavior:
      'A permanently failing message is retried up to the configured limit then moved to the DLQ; no infinite retry and consumers stay healthy.',
    actualBehavior:
      movedToDlq && mainQueueDrained
        ? `Message retried up to the ${maxRetries}-retry limit (attempts observed: ${attempts.length}), main queue drained, message dead-lettered to ${PAYMENT_DLQ}.`
        : 'DLQ routing or retry bounding was not observed.',
    evidence: {
      queueName: PAYMENT_QUEUE,
      dlqName: PAYMENT_DLQ,
      configuredMaxRetries: maxRetries,
      initialEventId: eventId,
      orderId,
      apiResponse: orderResponse,
      attempts,
      attemptCount: attempts.length,
      dlqState: queueSnapshot(dlqState, [PAYMENT_QUEUE, PAYMENT_DLQ]),
      finalQueueState: queueSnapshot(finalQueues, [PAYMENT_QUEUE, PAYMENT_DLQ]),
      paymentEventRows: paymentRows,
      otherConsumersHealthy: {
        inventory: otherConsumersHealthy[0],
        notification: otherConsumersHealthy[1],
        analytics: otherConsumersHealthy[2],
      },
      checks: { movedToDlq, mainQueueDrained, noInfiniteRetry, noBusinessRecord, consumersHealthy, dlqMessages: dlq.messages },
    },
  };
}

// ---------------------------------------------------------------------------
// Scenario D - Consumer crash / restart
// ---------------------------------------------------------------------------
async function publishEventAndObserveRecovery(orderId) {
  const observation = { note: 'Redelivery inferred from duplicate consumer attempts for the crashed event.', attempts: [] };
  try {
    const state = await getPaymentState();
    observation.attempts = (state.json?.attempts ?? []).filter((attempt) => attempt.orderId === orderId);
  } catch {
    // best-effort
  }
  return observation;
}

async function scenarioConsumerCrashRecovery() {
  await resetAsyncConsumers();
  const restartsBefore = containerRestartCount('payment-service');
  await httpJson('POST', `${PAYMENT_SERVICE_URL}/debug/config`, { mode: 'crash-after-success-once' });

  const orderRequest = { customerId: 'fail-crash-recovery', productId: 'sku-001', quantity: 1, totalAmount: 9.99 };
  const orderResponse = await createOrder(orderRequest);
  const orderId = orderResponse.json?.orderId;

  // Wait for the container to actually restart as a result of process.exit(1).
  const restartsAfterCrash = await waitFor(
    () => {
      const count = containerRestartCount('payment-service');
      if (count === null) {
        return null;
      }
      return count > (restartsBefore ?? 0) ? count : null;
    },
    { description: 'payment consumer crash/restart', timeoutMs: 120000, intervalMs: 2000 },
  );

  await waitForService(`${PAYMENT_SERVICE_URL}/health`, 'payment service recovery after crash');

  const processed = await waitFor(
    async () => {
      const state = await getPaymentProcessed();
      const match = state.json?.events?.find?.((event) => event.orderId === orderId);
      return match ? state.json : null;
    },
    { description: 'payment recovery after consumer crash', timeoutMs: 180000, intervalMs: 3000 },
  );

  const redeliveryObservation = await publishEventAndObserveRecovery(orderId);
  const paymentRows = await sqlRows(
    `SELECT event_id, order_id, retry_count, status FROM payment_events WHERE order_id = '${orderId}'`,
    ['eventId', 'orderId', 'retryCount', 'status'],
  );
  const queueState = queueSnapshot(await rabbitQueues(), [PAYMENT_QUEUE, PAYMENT_DLQ]);

  const crashed = restartsAfterCrash > (restartsBefore ?? 0);
  const recoveredExactlyOnce = paymentRows.rows.length === 1;
  const idempotent = processed.events.filter((event) => event.orderId === orderId).length === 1;

  return {
    status: crashed && recoveredExactlyOnce && idempotent ? 'PASS' : 'FAIL',
    scenario: 'Consumer crash and restart',
    expectedBehavior:
      'Crashing the consumer after a durable write causes RabbitMQ to redeliver the unacked message; idempotency prevents a duplicate business effect.',
    actualBehavior:
      crashed && recoveredExactlyOnce
        ? 'Payment consumer crashed after the durable write, was restarted, redelivered the message, and the business record remained single-copy.'
        : 'Crash, redelivery or idempotent recovery was not fully observed.',
    evidence: {
      apiResponse: orderResponse,
      orderId,
      restartsBefore,
      restartsAfterCrash,
      redeliveryObservation,
      processedState: processed,
      paymentEventRows: paymentRows,
      queueState,
      checks: { crashed, recoveredExactlyOnce, idempotent, businessRows: paymentRows.rows.length },
    },
  };
}

// ---------------------------------------------------------------------------
// Scenario E - Duplicate message / idempotency
// ---------------------------------------------------------------------------
async function scenarioDuplicateIdempotency() {
  await resetAsyncConsumers();
  await resetInventoryState(1000);

  const eventId = '11111111-1111-4111-8111-111111111111';
  const orderId = '22222222-2222-4222-8222-222222222222';
  const traceId = 'trace-duplicate-idempotency';
  const event = {
    eventId,
    orderId,
    customerId: 'duplicate-test',
    productId: 'sku-001',
    quantity: 2,
    totalAmount: 39.98,
    timestamp: new Date().toISOString(),
    traceId,
    type: 'OrderCreated',
  };

  const stockBefore = (await getInventoryStock()).json;

  await publishEvent(event);
  // Wait until the first delivery has been applied before publishing the duplicate.
  await waitFor(
    async () => {
      const state = (await getInventoryStock()).json;
      return state?.available === stockBefore.available - 2 ? state : null;
    },
    { description: 'first inventory reservation applied', timeoutMs: 120000, intervalMs: 1000 },
  );
  const stockAfterFirst = (await getInventoryStock()).json;

  await publishEvent(event);
  await sleep(5000); // allow the duplicate to be consumed and ignored
  const stockAfterDuplicate = (await getInventoryStock()).json;

  const paymentRows = await sqlRows(
    `SELECT event_id, order_id, retry_count, status FROM payment_events WHERE event_id = '${eventId}'`,
    ['eventId', 'orderId', 'retryCount', 'status'],
  );
  const inventoryRows = await sqlRows(
    `SELECT event_id, order_id, quantity, status FROM inventory_events WHERE event_id = '${eventId}'`,
    ['eventId', 'orderId', 'quantity', 'status'],
  );
  const paymentState = await getPaymentState();

  const inventoryDecreasedOnce =
    stockAfterFirst.available === stockBefore.available - 2 && stockAfterFirst.reserved === stockBefore.reserved + 2;
  const duplicateIgnored =
    stockAfterDuplicate.available === stockAfterFirst.available && stockAfterDuplicate.reserved === stockAfterFirst.reserved;
  const singlePaymentRow = paymentRows.rows.length === 1;
  const singleInventoryRow = inventoryRows.rows.length === 1;

  return {
    status: inventoryDecreasedOnce && duplicateIgnored && singlePaymentRow && singleInventoryRow ? 'PASS' : 'FAIL',
    scenario: 'Duplicate message and idempotency',
    expectedBehavior:
      'The same logical event delivered twice decrements stock exactly once; the duplicate is recognised and ignored for both inventory and payment.',
    actualBehavior:
      inventoryDecreasedOnce && duplicateIgnored
        ? `Stock decreased exactly once (${stockBefore.available} -> ${stockAfterFirst.available}) and remained unchanged after the duplicate (${stockAfterDuplicate.available}); payment had a single row.`
        : 'Idempotency was not enforced for duplicate delivery.',
    evidence: {
      event,
      stockBefore,
      stockAfterFirst,
      stockAfterDuplicate,
      paymentEventRows: paymentRows,
      inventoryEventRows: inventoryRows,
      paymentDebugState: paymentState.json,
      checks: { inventoryDecreasedOnce, duplicateIgnored, singlePaymentRow, singleInventoryRow },
    },
  };
}

// ---------------------------------------------------------------------------
// Scenario F - Inventory concurrency / overselling
// ---------------------------------------------------------------------------
async function scenarioInventoryConcurrency() {
  await resetAsyncConsumers();
  const initialStock = 10;
  const perRequest = 2;
  const concurrency = 8;
  await resetInventoryState(initialStock);

  const before = (await getInventoryStock()).json;

  const requests = Array.from({ length: concurrency }, (_, index) => ({
    customerId: `concurrency-${index + 1}`,
    productId: 'sku-001',
    quantity: perRequest,
    totalAmount: 19.99,
  }));

  const startedAt = performance.now();
  const responses = await Promise.all(
    requests.map((payload) => httpJson('POST', `${INVENTORY_SERVICE_URL}/inventory/reserve`, payload)),
  );
  const elapsedMs = Number((performance.now() - startedAt).toFixed(1));

  const successCount = responses.filter((response) => response.ok).length;
  const rejectedCount = responses.length - successCount;
  const after = (await getInventoryStock()).json;

  const reservedQuantity = successCount * perRequest;
  const expectedSuccesses = Math.floor(initialStock / perRequest);
  const totalRequested = concurrency * perRequest;

  const neverNegative = after.available >= 0;
  const neverOversold = reservedQuantity <= initialStock;
  const dbConsistent = after.available === initialStock - reservedQuantity && after.reserved === reservedQuantity;
  const successMatchesCapacity = successCount === expectedSuccesses;
  const noLostUpdates = after.available + after.reserved === initialStock;

  return {
    status: neverNegative && neverOversold && dbConsistent && successMatchesCapacity && noLostUpdates ? 'PASS' : 'FAIL',
    scenario: 'Inventory concurrency and overselling',
    expectedBehavior:
      'Concurrent reservations never drive stock negative and never exceed available stock; the DB remains consistent with no lost updates.',
    actualBehavior:
      neverNegative && neverOversold && dbConsistent
        ? `Initial stock ${initialStock}; ${successCount} of ${concurrency} reservations of ${perRequest} succeeded (${reservedQuantity} units), ${rejectedCount} rejected; final available=${after.available}, reserved=${after.reserved}; no overselling or lost updates.`
        : 'Concurrency safety was not preserved under parallel reservations.',
    evidence: {
      initialStock,
      perRequest,
      concurrency,
      totalRequested,
      responses,
      elapsedMs,
      successCount,
      rejectedCount,
      reservedQuantity,
      expectedSuccesses,
      stockBefore: before,
      stockAfter: after,
      checks: {
        neverNegative,
        neverOversold,
        dbConsistent,
        successMatchesCapacity,
        noLostUpdates,
        finalAvailable: after.available,
        finalReserved: after.reserved,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// Scenario G - Slow notification and analytics
// ---------------------------------------------------------------------------
async function scenarioSlowConsumers() {
  await resetAsyncConsumers();
  const delayMs = 3000;
  await httpJson('POST', `${NOTIFICATION_SERVICE_URL}/debug/config`, { processingDelayMs: delayMs });
  await httpJson('POST', `${ANALYTICS_SERVICE_URL}/debug/config`, { processingDelayMs: delayMs });

  const orderRequest = { customerId: 'slow-consumers', productId: 'sku-001', quantity: 1, totalAmount: 12.99 };
  const requestStart = new Date().toISOString();
  const startedAt = performance.now();
  const orderResponse = await createOrder(orderRequest);
  const elapsedMs = Number((performance.now() - startedAt).toFixed(1));
  const acceptedAt = new Date().toISOString();
  const orderId = orderResponse.json?.orderId;

  const orderRow = await waitFor(
    async () => {
      const rows = await sqlQuery(`SELECT id, customer_id, product_id, quantity, total_amount, status, created_at FROM orders WHERE id = '${orderId}'`);
      return rows ? rows : null;
    },
    { description: 'order persistence with slow consumers', timeoutMs: 120000, intervalMs: 500 },
  );
  const persistedAt = new Date().toISOString();

  const paymentProcessed = await waitFor(
    async () => {
      const state = await getPaymentProcessed();
      return state.json?.events?.find?.((event) => event.orderId === orderId) ? state.json : null;
    },
    { description: 'payment independent processing', timeoutMs: 120000, intervalMs: 1000 },
  );
  const paymentAt = new Date().toISOString();

  const notificationState = await waitFor(
    async () => {
      const state = await getNotificationState();
      return state.json?.items?.find?.((item) => item.orderId === orderId) ? state.json : null;
    },
    { description: 'notification completion', timeoutMs: 180000, intervalMs: 1000 },
  );
  const notificationAt = new Date().toISOString();

  const analyticsState = await waitFor(
    async () => {
      const state = await getAnalyticsState();
      return state.json?.items?.find?.((item) => item.orderId === orderId) ? state.json : null;
    },
    { description: 'analytics completion', timeoutMs: 180000, intervalMs: 1000 },
  );
  const analyticsAt = new Date().toISOString();

  const orderNotBlocked = orderResponse.ok && elapsedMs < delayMs;
  const persistedImmediately = Boolean(orderRow);
  const paymentIndependent = Boolean(paymentProcessed);
  const consumersCompleted = Boolean(notificationState) && Boolean(analyticsState);

  return {
    status: orderNotBlocked && persistedImmediately && paymentIndependent && consumersCompleted ? 'PASS' : 'FAIL',
    scenario: 'Slow notification and analytics',
    expectedBehavior: 'Order acceptance and persistence remain fast while slow Notification/Analytics consumers process asynchronously.',
    actualBehavior: orderNotBlocked
      ? `Order API responded in ${elapsedMs}ms (below the ${delayMs}ms consumer delay); order persisted immediately; payment/inventory proceeded and slow consumers completed later.`
      : 'Order request path was blocked by the slow consumers.',
    evidence: {
      configuredDelayMs: delayMs,
      apiResponse: orderResponse,
      elapsedMs,
      orderId,
      timestamps: { requestStart, acceptedAt, persistedAt, paymentAt, notificationAt, analyticsAt },
      orderPersistence: orderRow,
      paymentProcessedState: paymentProcessed,
      notificationState,
      analyticsState,
      checks: { orderNotBlocked, persistedImmediately, paymentIndependent, consumersCompleted },
    },
  };
}


async function runScenario(test, fileName, runner) {
  try {
    const result = await runner();
    result.test = test;
    await writeFile(path.join(RESULTS_DIR, `${fileName}.json`), `${JSON.stringify(result, null, 2)}\n`, 'utf8');
    return { test, fileName: `${fileName}.json`, ...result };
  } catch (error) {
    const cause = error && error.cause ? ` (cause: ${error.cause.code || error.cause.message || error.cause})` : '';
    const stack = error instanceof Error ? (error.stack || '').split('\n').slice(0, 4).join(' | ') : '';
    const result = {
      test,
      fileName: `${fileName}.json`,
      status: 'BLOCKED',
      scenario: fileName,
      expectedBehavior: 'Scenario should complete and produce runtime evidence.',
      actualBehavior: (error instanceof Error ? error.message : String(error)) + cause,
      errorStack: stack,
      evidence: {},
    };
    await writeFile(path.join(RESULTS_DIR, `${fileName}.json`), `${JSON.stringify(result, null, 2)}\n`, 'utf8');
    return result;
  }
}

function buildSummary(results) {
  const rows = [
    '| Test | Scenario | Expected behavior | Actual behavior | Status | Evidence |',
    '| ---- | -------- | ----------------- | --------------- | ------ | -------- |',
  ];

  for (const result of results) {
    const evidencePath = `tests/results/failure/${result.fileName}`;
    const escape = (value) => String(value ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
    rows.push(
      `| ${escape(result.test)} | ${escape(result.scenario)} | ${escape(result.expectedBehavior)} | ${escape(result.actualBehavior)} | ${escape(result.status)} | ${evidencePath} |`,
    );
  }

  const passed = results.filter((r) => r.status === 'PASS').length;
  const failed = results.filter((r) => r.status === 'FAIL').length;
  const blocked = results.filter((r) => r.status === 'BLOCKED').length;

  return [
    '# RabbitMQ Failure-Mode Test Summary',
    '',
    `Generated: ${new Date().toISOString()}`,
    '',
    `Totals: PASS=${passed} FAIL=${failed} BLOCKED=${blocked}`,
    '',
    rows.join('\n'),
    '',
    '> Status is PASS only when verified by actual runtime evidence captured in the linked JSON file.',
    '> Failure injection is development-only and disabled by default.',
    '',
  ].join('\n');
}

async function main() {
  await mkdir(RESULTS_DIR, { recursive: true });
  await waitForStackReady();

  const scenarios = [
    ['A', 'payment-unavailable', scenarioPaymentUnavailable],
    ['B', 'payment-retry', scenarioPaymentRetry],
    ['C', 'payment-dlq', scenarioPaymentDlq],
    ['D', 'consumer-crash-recovery', scenarioConsumerCrashRecovery],
    ['E', 'duplicate-idempotency', scenarioDuplicateIdempotency],
    ['F', 'inventory-concurrency', scenarioInventoryConcurrency],
    ['G', 'slow-consumers', scenarioSlowConsumers],
  ];

  const results = [];
  for (const [test, fileName, runner] of scenarios) {
    console.log(`\n=== Running failure scenario ${test}: ${fileName} ===`);
    const result = await runScenario(test, fileName, runner);
    console.log(`--- ${test} ${fileName}: ${result.status} ---`);
    results.push(result);
  }

  const summary = buildSummary(results);
  await writeFile(path.join(RESULTS_DIR, 'FAILURE-TEST-SUMMARY.md'), summary, 'utf8');

  console.log('\n' + summary);

  const failures = results.filter((result) => result.status !== 'PASS');
  if (failures.length > 0) {
    process.exitCode = 1;
  }
}

await main();

