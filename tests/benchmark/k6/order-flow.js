// Parameterised k6 workload for the synchronous-vs-RabbitMQ benchmark.
//
// Both modes hit the SAME gateway endpoint with the SAME payload. The only
// difference is the `x-benchmark-mode` header:
//   - async : gateway -> order-service persists + publishes, responds immediately
//   - sync  : gateway -> order-service -> payment -> inventory -> notification -> analytics
//
// Configuration via environment variables:
//   MODE        = async | sync            (required)
//   VUS         = number of concurrent virtual users (default 100)
//   DURATION    = load duration, e.g. 20s (default 20s)
//   GATEWAY_URL = base URL                  (default http://127.0.0.1:3000)
//
// Metrics of interest are the standard k6 http_req_* trend metrics plus the
// custom `order_requests` counter broken down by outcome.

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const MODE = __ENV.MODE || 'async';
const VUS = Number(__ENV.VUS || 100);
const DURATION = __ENV.DURATION || '20s';
const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://127.0.0.1:3000';

export const orderOutcomes = new Counter('order_outcomes');

export const options = {
  discardResponseBodies: false,
  scenarios: {
    order_flow: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
    },
  },
  thresholds: {},
  summaryTrendStats: ['min', 'avg', 'med', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const payload = JSON.stringify({
    customerId: `customer-${__VU}-${__ITER}`,
    productId: 'sku-001',
    quantity: 1,
    totalAmount: 249.99,
  });

  const res = http.post(`${GATEWAY_URL}/orders`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'x-benchmark-mode': MODE,
    },
    tags: { mode: MODE },
  });

  const ok = res.status >= 200 && res.status < 300;
  orderOutcomes.add(1, { outcome: ok ? 'success' : 'failure' });

  check(res, {
    'status is 2xx': (r) => r.status >= 200 && r.status < 300,
  });
}
