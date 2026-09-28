// End-to-end concurrency / load test for the async order pipeline.
//
// Fires N concurrent orders through the API gateway and verifies, after the
// asynchronous inventory consumer catches up, that stock is decremented once
// per accepted order with no negative stock and no lost updates.
//
// Usage: node tests/concurrency/run-load.mjs [orderCount]
import { execFileSync } from 'child_process';

const ROOT = process.cwd();
const GATEWAY = process.env.API_GATEWAY_URL ?? 'http://127.0.0.1:3000';
const PAYMENT = process.env.PAYMENT_SERVICE_URL ?? 'http://127.0.0.1:3002';
const INVENTORY = process.env.INVENTORY_SERVICE_URL ?? 'http://127.0.0.1:3003';
const NOTIFICATION = process.env.NOTIFICATION_SERVICE_URL ?? 'http://127.0.0.1:3004';
const ANALYTICS = process.env.ANALYTICS_SERVICE_URL ?? 'http://127.0.0.1:3005';
const ORDERS = Number(process.argv[2] ?? 200);
const INITIAL_STOCK = 1000;

function dockerCompose(args) {
  return execFileSync('docker', ['compose', ...args], { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 20 * 1024 * 1024 });
}
function sql(query) {
  return dockerCompose(['exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'ecommerce', '-t', '-A', '-F', '|', '-c', query]).trim();
}
async function post(url, body) {
  const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
  return { status: response.status };
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  sql(`UPDATE inventory_items SET available=${INITIAL_STOCK}, reserved=0 WHERE product_id='sku-001'; DELETE FROM inventory_events;`);
  await post(`${INVENTORY}/debug/reset`, {});
  await post(`${PAYMENT}/debug/reset`, {});
  await post(`${PAYMENT}/debug/config`, { mode: 'off', disableRandomFailures: true });
  await post(`${NOTIFICATION}/debug/config`, { processingDelayMs: 50 });
  await post(`${ANALYTICS}/debug/config`, { processingDelayMs: 50 });

  const startedAt = Date.now();
  const results = await Promise.all(
    Array.from({ length: ORDERS }, (_, i) => post(`${GATEWAY}/orders`, { customerId: `load-${i}`, productId: 'sku-001', quantity: 1, totalAmount: 20 })),
  );
  const accepted = results.filter((r) => r.status === 201).length;
  const elapsedMs = Date.now() - startedAt;

  let state = { available: -1, reserved: -1, events: -1 };
  for (let i = 0; i < 60; i += 1) {
    const [available, reserved] = sql("SELECT available, reserved FROM inventory_items WHERE product_id='sku-001';").split('|');
    const events = Number(sql('SELECT COUNT(*) FROM inventory_events;'));
    state = { available: Number(available), reserved: Number(reserved), events };
    if (events >= accepted) break;
    await sleep(1000);
  }

  const consistent = state.available + state.reserved === INITIAL_STOCK;
  const noNegative = state.available >= 0;
  const allReserved = state.reserved === accepted;

  console.log(JSON.stringify({ orders: ORDERS, accepted, rejected: ORDERS - accepted, elapsedMs, inventory: state, checks: { consistent, noNegative, allReserved } }, null, 2));

  if (!(consistent && noNegative && allReserved && accepted === ORDERS)) {
    process.exitCode = 1;
  }
}

await main();
